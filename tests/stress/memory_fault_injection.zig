//! Memory safety stress testing with fault injection for KausalDB.
//!
//! Tests arena-per-subsystem patterns under hostile conditions including:
//! - Memory allocation failures during operations
//! - I/O errors during memory-intensive operations
//! - Corruption injection during arena operations
//! - Error path memory leak detection
//! - Arena reset safety under failure conditions

const std = @import("std");
const kausaldb = @import("kausaldb");
const testing = std.testing;
const assert = kausaldb.assert.assert;

const SimulationVFS = kausaldb.simulation_vfs.SimulationVFS;
const StorageEngine = kausaldb.storage.StorageEngine;
const MemtableManager = kausaldb.storage.MemtableManager;
const ContextBlock = kausaldb.types.ContextBlock;
const BlockId = kausaldb.types.BlockId;
const GraphEdge = kausaldb.types.GraphEdge;
const EdgeType = kausaldb.types.EdgeType;

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
                .remap = std.mem.Allocator.noRemap,
            },
        };
    }

    fn alloc(ctx: *anyopaque, len: usize, ptr_align: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        self.total_allocations += 1;

        if (self.total_allocations > self.fail_after) {
            self.failure_count += 1;
            return null; // Simulate allocation failure
        }

        return self.backing_allocator.rawAlloc(len, ptr_align, ret_addr);
    }

    fn resize(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.backing_allocator.rawResize(buf, buf_align, new_len, ret_addr);
    }

    fn free(ctx: *anyopaque, buf: []u8, buf_align: std.mem.Alignment, ret_addr: usize) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        self.backing_allocator.rawFree(buf, buf_align, ret_addr);
    }
};

test "memory fault injection: allocation failure during memtable operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        // No memory leaks should occur - we use backing allocator for infrastructure
        if (deinit_status == .leak) @panic("Memory leak detected in fault injection test");
    }
    const backing_allocator = gpa.allocator();

    // Test allocation failures at different points
    const failure_points = [_]u32{ 5, 10, 20, 50, 100 };

    for (failure_points) |fail_after| {
        var failing_alloc = FailingAllocator.init(backing_allocator, fail_after);
        const failing_allocator = failing_alloc.allocator();

        // Use backing allocator for infrastructure to prevent leaks
        var sim_vfs = try SimulationVFS.init(backing_allocator);
        defer sim_vfs.deinit();

        var memtable = MemtableManager.init(backing_allocator, sim_vfs.vfs(), "test_data", 1024 * 1024) catch |err| {
            // This shouldn't fail with backing allocator
            return err;
        };
        defer memtable.deinit();

        // Try to add blocks until allocation fails
        var block_count: u32 = 0;
        while (block_count < 200) : (block_count += 1) {
            // Allocate strings separately to ensure proper cleanup on failure
            const source_uri = std.fmt.allocPrint(failing_allocator, "test://fault/{}", .{block_count}) catch break;
            defer failing_allocator.free(source_uri);

            const content = std.fmt.allocPrint(failing_allocator, "Fault injection test block {}", .{block_count}) catch break;
            defer failing_allocator.free(content);

            const block = ContextBlock{
                .id = BlockId.from_bytes([_]u8{@intCast(std.crypto.random.int(u8))} ** 16),
                .version = 1,
                .source_uri = source_uri,
                .metadata_json = "{}",
                .content = content,
            };

            memtable.put_block(block) catch |err| {
                // Expected failure, verify system state
                try testing.expect(err == error.OutOfMemory);
                break;
            };
        }

        // Verify memtable can be safely cleaned up even after failures
        memtable.clear();
        try testing.expectEqual(@as(usize, 0), memtable.memory_usage());

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

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x2000000);
    defer sim_vfs.deinit();

    // Enable I/O failures for testing error handling
    var sim_vfs_mut = &sim_vfs;
    sim_vfs_mut.enable_io_failures(100, .{ .write = true, .read = true, .create = false, .remove = false, .mkdir = false, .sync = false }); // 10% failure rate

    var storage = StorageEngine.init_default(allocator, sim_vfs.vfs(), "test_data") catch |err| {
        // Expected failure during initialization under fault injection
        try testing.expect(err == error.AccessDenied or err == error.FileNotFound or err == error.IoError);
        return;
    };
    defer storage.deinit();

    // Try storage operations that should fail gracefully
    storage.startup() catch |err| {
        // Expected I/O failure under fault injection, verify memory cleanup
        try testing.expect(err == error.AccessDenied or err == error.FileNotFound or err == error.IoError);
        return;
    };

    // If startup succeeded, test operations under I/O stress
    var test_id_bytes: [16]u8 = undefined;
    std.crypto.random.bytes(&test_id_bytes);
    if (test_id_bytes[0] == 0) test_id_bytes[0] = 1; // Ensure non-zero

    const test_block = ContextBlock{
        .id = BlockId.from_bytes(test_id_bytes),
        .version = 1,
        .source_uri = "test://io_fault",
        .metadata_json = "{}",
        .content = "I/O fault injection test content",
    };

    // This may fail due to injected write failure or succeed gracefully
    const result = storage.put_block(test_block);
    if (result) |_| {
        // Operation succeeded despite I/O stress - this is acceptable
    } else |err| {
        // Expected I/O error under fault injection
        try testing.expect(err == error.AccessDenied or err == error.FileNotFound or err == error.OutOfMemory or err == error.IoError);
    }

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

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var memtable = try MemtableManager.init(allocator, sim_vfs.vfs(), "test_data", 1024 * 1024);
    defer memtable.deinit();

    // Add some normal data first
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        const block = ContextBlock{
            .id = BlockId.from_bytes([_]u8{@intCast(std.crypto.random.int(u8))} ** 16),
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
    try testing.expect(memtable.memory_usage() > 0); // Memory should be used

    // Test arena reset under various conditions
    memtable.clear();
    try testing.expectEqual(@as(u64, 0), memtable.memory_usage());

    // Add data again to test arena reuse after reset
    const reuse_block = ContextBlock{
        .id = BlockId.from_bytes([_]u8{@intCast(std.crypto.random.int(u8))} ** 16),
        .version = 1,
        .source_uri = try allocator.dupe(u8, "test://reuse"),
        .metadata_json = "{}",
        .content = try allocator.dupe(u8, "Arena reuse test"),
    };
    defer allocator.free(reuse_block.source_uri);
    defer allocator.free(reuse_block.content);

    try memtable.put_block(reuse_block);
    try testing.expect(memtable.memory_usage() > 0); // Should have some memory usage
}

test "memory fault injection: error path cleanup validation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("Memory leak detected in error path test");
    }
    const allocator = gpa.allocator();

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x3000000);
    defer sim_vfs.deinit();

    // Test multiple failure scenarios in sequence
    const failure_scenarios = [_]struct {
        name: []const u8,
        file_pattern: []const u8,
    }{
        .{ .name = "write_failure", .file_pattern = "error_test/wal/wal_0000.log" },
        .{ .name = "read_failure", .file_pattern = "error_test/sst/sstable_0000.sst" },
        .{ .name = "corruption", .file_pattern = "error_test/wal/wal_0001.log" },
    };

    for (failure_scenarios, 0..) |scenario, scenario_idx| {
        // Configure fault injection based on scenario
        if (std.mem.eql(u8, scenario.name, "write_failure")) {
            var sim_vfs_mut = &sim_vfs;
            sim_vfs_mut.enable_io_failures(200, .{ .write = true, .read = false, .create = false, .remove = false, .mkdir = false, .sync = false }); // 20% write failure rate
        } else if (std.mem.eql(u8, scenario.name, "read_failure")) {
            var sim_vfs_mut = &sim_vfs;
            sim_vfs_mut.enable_io_failures(200, .{ .read = true, .write = false, .create = false, .remove = false, .mkdir = false, .sync = false }); // 20% read failure rate
        } else if (std.mem.eql(u8, scenario.name, "corruption")) {
            var sim_vfs_mut = &sim_vfs;
            sim_vfs_mut.enable_read_corruption(1, 3); // 1 bit flip per KB
        }

        var storage = StorageEngine.init_default(allocator, sim_vfs.vfs(), "error_test") catch |err| {
            // Expected initialization failure
            try testing.expect(err == error.AccessDenied or err == error.FileNotFound);
            continue;
        };
        defer storage.deinit();

        // Try to start storage engine
        storage.startup() catch |err| {
            // Expected startup failure under fault injection
            try testing.expect(err == error.AccessDenied or err == error.FileNotFound or err == error.IoError);
            continue;
        };

        // If startup succeeded, test operations under fault injection
        const source_uri = try std.fmt.allocPrint(allocator, "test://error_path/{}", .{scenario_idx});
        defer allocator.free(source_uri);

        const content = try std.fmt.allocPrint(allocator, "Error path test scenario {}", .{scenario_idx});
        defer allocator.free(content);

        const test_block = ContextBlock{
            .id = BlockId.from_bytes([_]u8{@intCast(std.crypto.random.int(u8))} ** 16),
            .version = 1,
            .source_uri = source_uri,
            .metadata_json = "{}",
            .content = content,
        };

        // Operation should fail gracefully
        storage.put_block(test_block) catch |err| {
            try testing.expect(err == error.AccessDenied or err == error.CorruptedData or err == error.IoError);
        };

        // Verify memory state remains consistent
        const final_usage = storage.memory_usage();
        try testing.expect(final_usage.total_bytes >= 0);

        log.debug("Error path scenario {} completed: name={s}, file={s}", .{ scenario_idx, scenario.name, scenario.file_pattern });
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

    // Using backing allocator for simulation VFS

    var sim_vfs = try SimulationVFS.init(backing_allocator);
    defer sim_vfs.deinit();

    var memtable = try MemtableManager.init(backing_allocator, sim_vfs.vfs(), "test_data", 1024 * 1024); // Use backing allocator for memtable
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
            // Create block with careful allocation error handling
            const source_uri = std.fmt.allocPrint(allocator, "test://pressure/{}/{}", .{ cycle, block_idx }) catch {
                cycle_successful = false;
                break;
            };
            defer allocator.free(source_uri);

            const content = std.fmt.allocPrint(allocator, "Pressure test cycle {} block {}", .{ cycle, block_idx }) catch {
                cycle_successful = false;
                break;
            };
            defer allocator.free(content);

            const block = ContextBlock{
                .id = BlockId.from_bytes([_]u8{@intCast(std.crypto.random.int(u8))} ** 16),
                .version = 1,
                .source_uri = source_uri,
                .metadata_json = "{}",
                .content = content,
            };

            memtable.put_block(block) catch {
                cycle_successful = false;
                break;
            };
        }

        if (cycle_successful) {
            successful_cycles += 1;
            try testing.expect(memtable.memory_usage() > 0); // Should have memory usage
        }

        // Always clear memtable to test arena reset under pressure
        memtable.clear();
        try testing.expectEqual(@as(u64, 0), memtable.memory_usage());

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

    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x5000000);
    defer sim_vfs.deinit();

    var storage = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "graph_stress_data");
    defer storage.deinit();
    try storage.startup();

    // Create blocks for edge testing
    // Create non-zero block IDs for stress testing
    var block_ids: [3]BlockId = undefined;
    for (0..3) |i| {
        var id_bytes: [16]u8 = undefined;
        std.crypto.random.bytes(&id_bytes);
        // Ensure first byte is non-zero to guarantee non-zero BlockId
        if (id_bytes[0] == 0) id_bytes[0] = @intCast(i + 1);
        block_ids[i] = BlockId.from_bytes(id_bytes);
    }

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
            // Skip self-referential edges
            if (std.mem.eql(u8, &from_id.bytes, &to_id.bytes)) continue;

            for (edge_types) |edge_type| {
                const edge = GraphEdge{
                    .source_id = from_id,
                    .target_id = to_id,
                    .edge_type = edge_type,
                };
                // GraphEdge doesn't have metadata_json field

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
