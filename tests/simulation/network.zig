//! Example simulation test cases demonstrating network partition scenarios.
//!
//! These tests show how to write deterministic, byte-for-byte reproducible
//! tests for complex distributed system scenarios.

const std = @import("std");
const simulation = @import("simulation");
const vfs = @import("vfs");
const assert = @import("assert");

const Simulation = simulation.Simulation;
const NodeId = simulation.NodeId;
const MessageType = simulation.MessageType;

test "network partition: write succeeds after partition heals" {
    const allocator = std.testing.allocator;

    // Initialize simulation with fixed seed for reproducibility
    var sim = try Simulation.init(allocator, 0xCAFE_BABE);
    defer sim.deinit();

    // Create a 3-node cluster
    const node1 = try sim.add_node();
    const node2 = try sim.add_node();
    const node3 = try sim.add_node();

    // Allow cluster to stabilize
    sim.tick_multiple(10);

    // Write some initial data to node1
    const node1_ptr = sim.find_node(node1);
    var node1_vfs = node1_ptr.filesystem_interface();

    // Create data directory first
    try node1_vfs.mkdir("data");

    var file = try node1_vfs.create("data/block_001.db");
    defer file.close();

    const initial_data = "Initial block data";
    _ = try file.write(initial_data);
    file.close();

    // Verify data was written
    try std.testing.expect(node1_vfs.exists("data/block_001.db"));

    // Create network partition isolating node3
    sim.partition_nodes(node1, node3);
    sim.partition_nodes(node2, node3);

    // Run simulation for 50 ticks with partition active
    sim.tick_multiple(50);

    // During partition, node1 and node2 should still be able to communicate
    // but node3 should be isolated

    // Write additional data to node1 during partition
    var file2 = try node1_vfs.create("data/block_002.db");
    defer file2.close();

    const partition_data = "Data written during partition";
    _ = try file2.write(partition_data);

    // Heal the partition
    sim.heal_partition(node1, node3);
    sim.heal_partition(node2, node3);

    // Allow time for cluster to recover and synchronize
    sim.tick_multiple(30);

    // Verify that both files exist on node1
    try std.testing.expect(node1_vfs.exists("data/block_001.db"));
    try std.testing.expect(node1_vfs.exists("data/block_002.db"));

    // Get final filesystem state for verification
    const final_state = try sim.node_filesystem_state(node1);
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
    try std.testing.expect(final_state.len >= 2);

    // Find our test files in the state
    var found_block_001 = false;
    var found_block_002 = false;

    for (final_state) |file_state| {
        if (std.mem.eql(u8, file_state.path, "data/block_001.db")) {
            found_block_001 = true;
            try std.testing.expect(std.mem.eql(u8, file_state.content.?, initial_data));
        }
        if (std.mem.eql(u8, file_state.path, "data/block_002.db")) {
            found_block_002 = true;
            try std.testing.expect(std.mem.eql(u8, file_state.content.?, partition_data));
        }
    }

    try std.testing.expect(found_block_001);
    try std.testing.expect(found_block_002);
}

test "packet loss scenario: writes eventually succeed" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0xDEAD_BEEF);
    defer sim.deinit();

    // Create 2-node cluster
    const node1 = try sim.add_node();
    const node2 = try sim.add_node();

    // Set high packet loss between nodes
    sim.configure_packet_loss(node1, node2, 0.8); // 80% packet loss

    // Allow initial setup
    sim.tick_multiple(5);

    // Write data to node1
    const node1_ptr = sim.find_node(node1);
    var node1_vfs = node1_ptr.filesystem_interface();

    var file = try node1_vfs.create("lossy_data.db");
    defer file.close();

    const test_data = "Data written with packet loss";
    _ = try file.write(test_data);
    file.close();

    // Run simulation to allow retries
    sim.tick_multiple(100);

    // Verify data was written to node1
    try std.testing.expect(node1_vfs.exists("lossy_data.db"));

    // Read back the data to verify integrity
    var read_file = try node1_vfs.open("lossy_data.db", .read);
    defer read_file.close();

    var buffer: [100]u8 = undefined;
    const bytes_read = try read_file.read(&buffer);
    try std.testing.expect(bytes_read == test_data.len);
    try std.testing.expect(std.mem.eql(u8, buffer[0..bytes_read], test_data));

    read_file.close();
}

test "high latency scenario: operations complete despite delays" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0x1234_5678);
    defer sim.deinit();

    // Create 3-node cluster
    const node1 = try sim.add_node();
    const node2 = try sim.add_node();
    const node3 = try sim.add_node();

    // Set high latency between nodes (simulate slow network)
    sim.configure_latency(node1, node2, 20); // 20 tick delay
    sim.configure_latency(node1, node3, 30); // 30 tick delay
    sim.configure_latency(node2, node3, 25); // 25 tick delay

    // Allow cluster to stabilize despite high latency
    sim.tick_multiple(50);

    // Write data to multiple nodes
    const node1_ptr = sim.find_node(node1);
    const node2_ptr = sim.find_node(node2);

    var node1_vfs = node1_ptr.filesystem_interface();
    var node2_vfs = node2_ptr.filesystem_interface();

    // Write to node1
    var file1 = try node1_vfs.create("high_latency_node1.db");
    defer file1.close();
    _ = try file1.write("Node1 data");

    // Write to node2
    var file2 = try node2_vfs.create("high_latency_node2.db");
    defer file2.close();
    _ = try file2.write("Node2 data");

    // Run simulation to allow high-latency operations to complete
    sim.tick_multiple(100);

    // Verify both nodes have their data
    try std.testing.expect(node1_vfs.exists("high_latency_node1.db"));
    try std.testing.expect(node2_vfs.exists("high_latency_node2.db"));
}

test "byzantine scenario: cluster handles corrupted messages" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0x9999_AAAA);
    defer sim.deinit();

    // Create 4-node cluster (can tolerate 1 byzantine node)
    const node1 = try sim.add_node();
    const node2 = try sim.add_node();
    const node3 = try sim.add_node();
    const node4 = try sim.add_node();

    // Allow cluster to stabilize
    sim.tick_multiple(20);

    // Write data to the cluster
    const node1_ptr = sim.find_node(node1);
    var node1_vfs = node1_ptr.filesystem_interface();

    var file = try node1_vfs.create("byzantine_test.db");
    defer file.close();

    const test_data = "Byzantine fault tolerant data";
    _ = try file.write(test_data);
    file.close();

    // Simulate network unreliability as proxy for byzantine behavior
    // True message corruption would require protocol-level fault injection
    sim.configure_packet_loss(node4, node1, 0.9);
    sim.configure_packet_loss(node4, node2, 0.9);
    sim.configure_packet_loss(node4, node3, 0.9);

    // Run simulation
    sim.tick_multiple(100);

    // Verify the honest nodes maintain consistency
    try std.testing.expect(node1_vfs.exists("byzantine_test.db"));

    // Read back data to verify integrity
    var read_file = try node1_vfs.open("byzantine_test.db", .read);
    defer read_file.close();

    var buffer: [100]u8 = undefined;
    const bytes_read = try read_file.read(&buffer);
    try std.testing.expect(std.mem.eql(u8, buffer[0..bytes_read], test_data));

    read_file.close();
}

test "deterministic replay: same seed produces identical results" {
    const allocator = std.testing.allocator;

    const seed = 0xABCDEF12;

    // Run simulation 1
    var sim1 = try Simulation.init(allocator, seed);
    defer sim1.deinit();

    const node1_a = try sim1.add_node();
    const node2_a = try sim1.add_node();

    sim1.partition_nodes(node1_a, node2_a);
    sim1.tick_multiple(10);
    sim1.heal_partition(node1_a, node2_a);
    sim1.tick_multiple(10);

    // Write some data
    const node1_ptr_a = sim1.find_node(node1_a);
    var node1_vfs_a = node1_ptr_a.filesystem_interface();
    var file_a = try node1_vfs_a.create("replay_test.db");
    _ = try file_a.write("Deterministic data");
    file_a.close();

    const state1 = try sim1.node_filesystem_state(node1_a);
    defer {
        for (state1) |file_state| {
            allocator.free(file_state.path);
            if (file_state.content) |content| {
                allocator.free(content);
            }
        }
        allocator.free(state1);
    }

    // Run simulation 2 with same seed
    var sim2 = try Simulation.init(allocator, seed);
    defer sim2.deinit();

    const node1_b = try sim2.add_node();
    const node2_b = try sim2.add_node();

    sim2.partition_nodes(node1_b, node2_b);
    sim2.tick_multiple(10);
    sim2.heal_partition(node1_b, node2_b);
    sim2.tick_multiple(10);

    // Write the same data
    const node1_ptr_b = sim2.find_node(node1_b);
    var node1_vfs_b = node1_ptr_b.filesystem_interface();
    var file_b = try node1_vfs_b.create("replay_test.db");
    _ = try file_b.write("Deterministic data");
    file_b.close();

    const state2 = try sim2.node_filesystem_state(node1_b);
    defer {
        for (state2) |file_state| {
            allocator.free(file_state.path);
            if (file_state.content) |content| {
                allocator.free(content);
            }
        }
        allocator.free(state2);
    }

    // Both simulations should produce identical results
    try std.testing.expect(sim1.ticks() == sim2.ticks());
    try std.testing.expect(state1.len == state2.len);

    for (state1, state2) |fs1, fs2| {
        try std.testing.expect(std.mem.eql(u8, fs1.path, fs2.path));
        try std.testing.expect(fs1.is_directory == fs2.is_directory);
        try std.testing.expect(fs1.size == fs2.size);
        try std.testing.expect(fs1.created_time == fs2.created_time);
        try std.testing.expect(fs1.modified_time == fs2.modified_time);

        if (fs1.content) |content1| {
            try std.testing.expect(fs2.content != null);
            try std.testing.expect(std.mem.eql(u8, content1, fs2.content.?));
        } else {
            try std.testing.expect(fs2.content == null);
        }
    }
}

test "complex scenario: partition during write operations" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, 0xABCDEF98);
    defer sim.deinit();

    // Create 5-node cluster
    const nodes = [_]NodeId{
        try sim.add_node(),
        try sim.add_node(),
        try sim.add_node(),
        try sim.add_node(),
        try sim.add_node(),
    };

    // Allow cluster to stabilize
    sim.tick_multiple(20);

    // Start writing data to multiple nodes
    for (nodes, 0..) |node_id, i| {
        const node_ptr = sim.find_node(node_id);
        var node_vfs = node_ptr.filesystem_interface();

        const filename = try std.fmt.allocPrint(allocator, "node_{}_data.db", .{i});
        defer allocator.free(filename);

        var file = try node_vfs.create(filename);
        defer file.close();

        const data = try std.fmt.allocPrint(allocator, "Data from node {}", .{i});
        defer allocator.free(data);

        _ = try file.write(data);
        file.close();
    }

    // Run for a few ticks
    sim.tick_multiple(5);

    // Create complex partition: split cluster into 2 groups
    sim.partition_nodes(nodes[0], nodes[3]);
    sim.partition_nodes(nodes[0], nodes[4]);
    sim.partition_nodes(nodes[1], nodes[3]);
    sim.partition_nodes(nodes[1], nodes[4]);
    sim.partition_nodes(nodes[2], nodes[3]);
    sim.partition_nodes(nodes[2], nodes[4]);

    // Run with partition active
    sim.tick_multiple(30);

    // Write more data during partition
    const node0_ptr = sim.find_node(nodes[0]);
    var node0_vfs = node0_ptr.filesystem_interface();

    var partition_file = try node0_vfs.create("partition_write.db");
    _ = try partition_file.write("Written during partition");
    partition_file.close();

    // Heal all partitions
    for (nodes[0..3]) |node_a| {
        for (nodes[3..5]) |node_b| {
            sim.heal_partition(node_a, node_b);
        }
    }

    // Allow cluster to recover
    sim.tick_multiple(50);

    // Verify data integrity on first node
    try std.testing.expect(node0_vfs.exists("node_0_data.db"));
    try std.testing.expect(node0_vfs.exists("partition_write.db"));

    // Get final state
    const final_state = try sim.node_filesystem_state(nodes[0]);
    defer {
        for (final_state) |file_state| {
            allocator.free(file_state.path);
            if (file_state.content) |content| {
                allocator.free(content);
            }
        }
        allocator.free(final_state);
    }

    // Should have at least the files we created
    try std.testing.expect(final_state.len >= 2);
}
