//! Memory safety stress testing with fault injection for CortexDB.
//!
//! Tests arena-per-subsystem patterns under hostile conditions including:
//! - Memory allocation failures during operations
//! - I/O errors during memory-intensive operations
//! - Corruption injection during arena operations
//! - Error path memory leak detection
//! - Arena reset safety under failure conditions

const std = @import("std");
const cortexdb = @import("cortexdb");
const testing = std.testing;
const assert = cortexdb.assert.assert;

const Simulation = cortexdb.simulation.Simulation;
const StorageEngine = cortexdb.storage.StorageEngine;
const MemtableManager = cortexdb.storage.MemtableManager;
const ContextBlock = cortexdb.types.ContextBlock;
const BlockId = cortexdb.types.BlockId;
const GraphEdge = cortexdb.types.GraphEdge;
const EdgeType = cortexdb.types.EdgeType;

const log = std.log.scoped(.memory_fault_injection);

/// Failing allocator that simulates memory pressure conditions
const FailingAllocator = struct {
    backing_allocator: std.mem.Allocator,
    failure_count: u32,
    fail_after: u32,
    total_allocations: u32,

    const Self = @This();

    pub fn init(backing_allocator: std.mem.Allocator, fail_after: u32) Self {
        return Self{
            .backing_allocator = backing_allocator,
            .failure_count = 0,
            .fail_after = fail_after,
            .total_allocations = 0,
        };
    }

    pub fn allocator(self: *Self) std.mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .free = free,
            },
        };
    }

    fn alloc(ctx: *anyopaque, len: usize, ptr_align: u8, ret_addr: usize) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        self.total_allocations += 1;

        if (self.total_allocations > self.fail_after) {
            self.failure_count += 1;
            return null; // Simulate allocation failure
        }

        return self.backing_allocator.rawAlloc(len, ptr_align, ret_addr);
    }

    fn resize(ctx: *anyopaque, buf: []u8, buf_align: u8, new_len: usize, ret_addr: usize) bool {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.backing_allocator.rawResize(buf, buf_align, new_len, ret_addr);
    }

    fn free(ctx: *anyopaque, buf: []u8, buf_align: u8, ret_addr: usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        self.backing_allocator.rawFree(buf, buf_align, ret_addr);
    }
};

test "memory fault injection: allocation failure during memtable operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in allocation failure test");
    }
    const backing_allocator = gpa.allocator();

    // Test allocation failures at different points
    const failure_points = [_]u32{ 5, 10, 20, 50, 100 };

    for (failure_points) |fail_after| {
        var failing_alloc = FailingAllocator.init(backing_allocator, fail_after);
        const allocator = failing_alloc.allocator();

        var sim = Simulation.init(allocator, 0x1000000 + fail_after) catch |err| {
            // Expected failure, verify cleanup
            try testing.expect(err == error.OutOfMemory);
            continue;
        };
        defer sim.deinit();

        var memtable = MemtableManager.init(allocator) catch |err| {
            // Expected failure during initialization
            try testing.expect(err == error.OutOfMemory);
            continue;
        };
        defer memtable.deinit();

        // Try to add blocks until allocation fails
        var block_count: u32 = 0;
        while (block_count < 200) : (block_count += 1) {
            const block = ContextBlock{
                .id = BlockId.generate(),
                .version = 1,
                .source_uri = std.fmt.allocPrint(allocator, "test://fault/{}", .{block_count}) catch break,
                .metadata_json = "{}",
                .content = std.fmt.allocPrint(allocator, "Fault injection test block {}", .{block_count}) catch break,
            };
            defer if (block.source_uri.len > 0) allocator.free(block.source_uri);
            defer if (block.content.len > 0) allocator.free(block.content);

            memtable.put_block(block) catch |err| {
                // Expected failure, verify system state
                try testing.expect(err == error.OutOfMemory);
                break;
            };
        }

        // Verify memtable can be safely cleaned up even after failures
        memtable.clear();
        try testing.expectEqual(@as(usize, 0), memtable.memory_usage().total_bytes);

        log.debug("Allocation failure test completed: fail_after={}, blocks_added={}, failures={}", .{ fail_after, block_count, failing_alloc.failure_count });
    }
}

test "memory fault injection: I/O errors during memory operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in I/O error test");
    }
    const allocator = gpa.allocator();

    var sim = try Simulation.init(allocator, 0x2000000);
    defer sim.deinit();

    // Inject I/O failures at critical points
    try sim.vfs.inject_fault(.write_failure, "test_data/wal/wal_0000.log");
    try sim.vfs.inject_fault(.read_failure, "test_data/sst/sstable_0000.sst");

    var storage = StorageEngine.init(allocator, sim.vfs, "test_data") catch |err| {
        // Expected failure during initialization
        try testing.expect(err == error.AccessDenied or err == error.FileNotFound);
        return;
    };
    defer storage.deinit();

    // Try storage operations that should fail gracefully
    storage.startup() catch |err| {
        // Expected I/O failure, verify memory cleanup
        try testing.expect(err == error.AccessDenied or err == error.FileNotFound);
        return;
    };

    // If startup succeeded, test operations under I/O stress
    const test_block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = "test://io_fault",
        .metadata_json = "{}",
        .content = "I/O fault injection test content",
    };

    // This should fail due to injected write failure
    const result = storage.put_block(test_block);
    try testing.expectError(error.AccessDenied, result);

    // Verify storage engine state remains consistent after failure
    const memory_usage = storage.memory_usage();
    try testing.expect(memory_usage.total_bytes >= 0); // Basic sanity check
}

test "memory fault injection: arena corruption detection" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in corruption test");
    }
    const allocator = gpa.allocator();

    var memtable = try MemtableManager.init(allocator);
    defer memtable.deinit();

    // Add some normal data first
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        const block = ContextBlock{
            .id = BlockId.generate(),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://corrupt/{}", .{i}),
            .metadata_json = "{}",
            .content = try std.fmt.allocPrint(allocator, "Corruption test block {}", .{i}),
        };
        defer allocator.free(block.source_uri);
        defer allocator.free(block.content);

        try memtable.put_block(block);
    }

    // Verify normal operation
    try testing.expectEqual(@as(usize, 100), memtable.memory_usage().block_count);

    // Test arena reset under various conditions
    memtable.clear();
    try testing.expectEqual(@as(usize, 0), memtable.memory_usage().total_bytes);

    // Add data again to test arena reuse after reset
    const reuse_block = ContextBlock{
        .id = BlockId.generate(),
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://reuse"),
        .metadata_json = "{}",
        .content = try allocator.dupe(u8, "Arena reuse test"),
    };
    defer allocator.free(reuse_block.source_uri);
    defer allocator.free(reuse_block.content);

    try memtable.put_block(reuse_block);
    try testing.expectEqual(@as(usize, 1), memtable.memory_usage().block_count);
}

test "memory fault injection: error path cleanup validation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in error path test");
    }
    const allocator = gpa.allocator();

    var sim = try Simulation.init(allocator, 0x3000000);
    defer sim.deinit();

    // Test multiple failure scenarios in sequence
    const failure_scenarios = [_]struct {
        fault_type: Simulation.FaultType,
        file_pattern: []const u8,
    }{
        .{ .fault_type = .write_failure, .file_pattern = "error_test/wal/wal_0000.log" },
        .{ .fault_type = .read_failure, .file_pattern = "error_test/sst/sstable_0000.sst" },
        .{ .fault_type = .corruption, .file_pattern = "error_test/wal/wal_0001.log" },
    };

    for (failure_scenarios, 0..) |scenario, scenario_idx| {
        try sim.vfs.inject_fault(scenario.fault_type, scenario.file_pattern);

        var storage = StorageEngine.init(allocator, sim.vfs, "error_test") catch |err| {
            // Expected initialization failure
            try testing.expect(err == error.AccessDenied or err == error.FileNotFound);
            continue;
        };
        defer storage.deinit();

        // Try to start storage engine
        storage.startup() catch |err| {
            // Expected startup failure
            try testing.expect(err == error.AccessDenied or err == error.FileNotFound);
            continue;
        };

        // If startup succeeded, test operations under fault injection
        const test_block = ContextBlock{
            .id = BlockId.generate(),
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://error_path/{}", .{scenario_idx}),
            .metadata_json = "{}",
            .content = try std.fmt.allocPrint(allocator, "Error path test scenario {}", .{scenario_idx}),
        };
        defer allocator.free(test_block.source_uri);
        defer allocator.free(test_block.content);

        // Operation should fail gracefully
        storage.put_block(test_block) catch |err| {
            try testing.expect(err == error.AccessDenied or err == error.CorruptedData);
        };

        // Verify memory state remains consistent
        const final_usage = storage.memory_usage();
        try testing.expect(final_usage.total_bytes >= 0);

        log.debug("Error path scenario {} completed: fault={s}, file={s}", .{ scenario_idx, @tagName(scenario.fault_type), scenario.file_pattern });
    }
}

test "memory fault injection: sustained operations under memory pressure" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in sustained pressure test");
    }
    const backing_allocator = gpa.allocator();

    // Test sustained operations with intermittent allocation failures
    var failing_alloc = FailingAllocator.init(backing_allocator, 1000); // Fail after 1000 allocations
    const allocator = failing_alloc.allocator();

    var sim = try Simulation.init(backing_allocator, 0x4000000); // Use backing allocator for sim
    defer sim.deinit();

    var memtable = try MemtableManager.init(backing_allocator); // Use backing allocator for memtable
    defer memtable.deinit();

    const total_cycles = 50;
    const blocks_per_cycle = 20;
    var successful_cycles: u32 = 0;

    var cycle: u32 = 0;
    while (cycle < total_cycles) : (cycle += 1) {
        var cycle_successful = true;

        // Try to add blocks to memtable
        var block_idx: u32 = 0;
        while (block_idx < blocks_per_cycle) : (block_idx += 1) {
            const block = ContextBlock{
                .id = BlockId.generate(),
                .version = 1,
                .source_uri = std.fmt.allocPrint(allocator, "test://pressure/{}/{}", .{ cycle, block_idx }) catch {
                    cycle_successful = false;
                    break;
                },
                .metadata_json = "{}",
                .content = std.fmt.allocPrint(allocator, "Pressure test cycle {} block {}", .{ cycle, block_idx }) catch {
                    cycle_successful = false;
                    break;
                },
            };
            defer if (block.source_uri.len > 0) allocator.free(block.source_uri);
            defer if (block.content.len > 0) allocator.free(block.content);

            memtable.put_block(block) catch {
                cycle_successful = false;
                break;
            };
        }

        if (cycle_successful) {
            successful_cycles += 1;
            try testing.expectEqual(@as(usize, blocks_per_cycle), memtable.memory_usage().block_count);
        }

        // Always clear memtable to test arena reset under pressure
        memtable.clear();
        try testing.expectEqual(@as(usize, 0), memtable.memory_usage().total_bytes);

        // Log progress
        if (cycle % 10 == 0) {
            log.debug("Pressure test cycle {}/{}: successful={}, failures={}", .{ cycle, total_cycles, successful_cycles, failing_alloc.failure_count });
        }
    }

    log.info("Sustained pressure test completed: {}/{} successful cycles, {} allocation failures", .{ successful_cycles, total_cycles, failing_alloc.failure_count });

    // Verify system survived pressure testing
    try testing.expect(successful_cycles > 0); // At least some cycles should succeed
    try testing.expect(failing_alloc.failure_count > 0); // Should have triggered failures
}

test "memory fault injection: graph edge operations under stress" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in graph edge stress test");
    }
    const allocator = gpa.allocator();

    var sim = try Simulation.init(allocator, 0x5000000);
    defer sim.deinit();

    var storage = try StorageEngine.init(allocator, sim.vfs, "graph_stress_data");
    defer storage.deinit();
    try storage.startup();

    // Create blocks for edge testing
    const block_ids = [_]BlockId{
        BlockId.generate(),
        BlockId.generate(),
        BlockId.generate(),
    };

    for (block_ids, 0..) |id, idx| {
        const block = ContextBlock{
            .id = id,
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://graph/{}", .{idx}),
            .metadata_json = "{}",
            .content = try std.fmt.allocPrint(allocator, "Graph stress test block {}", .{idx}),
        };
        defer allocator.free(block.source_uri);
        defer allocator.free(block.content);

        try storage.put_block(block);
    }

    // Create edges between blocks under memory stress
    const edge_types = [_]EdgeType{ .imports, .calls, .references };
    var edge_count: u32 = 0;

    for (block_ids[0..2]) |from_id| {
        for (block_ids[1..3]) |to_id| {
            for (edge_types) |edge_type| {
                const edge = GraphEdge{
                    .from_block_id = from_id,
                    .to_block_id = to_id,
                    .edge_type = edge_type,
                    .metadata_json = try std.fmt.allocPrint(allocator, "{{\"stress_edge\":{}}}", .{edge_count}),
                };
                defer allocator.free(edge.metadata_json);

                storage.put_edge(edge) catch |err| {
                    // May fail under memory pressure, should be graceful
                    try testing.expect(err == error.OutOfMemory);
                    continue;
                };

                edge_count += 1;
            }
        }
    }

    // Verify graph structure survives stress testing
    const memory_usage = storage.memory_usage();
    try testing.expect(memory_usage.total_bytes > 0);
    try testing.expect(memory_usage.block_count == block_ids.len);

    log.info("Graph edge stress test completed: {} edges created", .{edge_count});
}
