//! Main entry point and CLI interface.

const std = @import("std");
const custom_assert = @import("core/assert.zig");
const assert = custom_assert.assert;
const log = std.log.scoped(.main);
const storage_mod = @import("storage/engine.zig");
const query_engine = @import("query/engine.zig");
const context_block = @import("core/types.zig");
const vfs = @import("core/vfs.zig");
const production_vfs = @import("core/production_vfs.zig");
const concurrency = @import("core/concurrency.zig");
const server = @import("server/handler.zig");

const StorageEngine = storage_mod.StorageEngine;
const QueryEngine = query_engine.QueryEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

pub fn main() !void {
    // Initialize concurrency model first
    concurrency.init();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        try print_usage();
        return;
    }

    const command = args[1];

    if (std.mem.eql(u8, command, "version")) {
        try print_version();
    } else if (std.mem.eql(u8, command, "help")) {
        try print_usage();
    } else if (std.mem.eql(u8, command, "server")) {
        try run_server(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "demo")) {
        try run_demo(allocator);
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
        try print_usage();
        std.process.exit(1);
    }
}

fn print_version() !void {
    std.debug.print("KausalDB v0.1.0\n", .{});
}

fn print_usage() !void {
    std.debug.print(
        \\KausalDB - High-performance context database
        \\
        \\Usage:
        \\  kausaldb <command> [options]
        \\
        \\Commands:
        \\  version    Show version information
        \\  help       Show this help message
        \\  server     Start the database server
        \\  demo       Run a storage and query demonstration
        \\
        \\Examples:
        \\  kausaldb server --port 8080
        \\  kausaldb demo
        \\  kausaldb version
        \\
    , .{});
}

fn run_server(allocator: std.mem.Allocator, args: [][:0]u8) !void {
    _ = args;

    std.debug.print("KausalDB server starting...\n", .{});

    var prod_vfs = production_vfs.ProductionVFS.init(allocator);
    const vfs_interface = prod_vfs.vfs();
    const cwd = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(cwd);
    const data_dir = try std.fs.path.join(allocator, &[_][]const u8{ cwd, "kausaldb_data" });
    defer allocator.free(data_dir);

    // Ensure data directory exists before initializing storage engine
    vfs_interface.mkdir_all(data_dir) catch |err| switch (err) {
        vfs.VFSError.FileExists => {}, // Directory already exists, continue
        else => return err,
    };

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.startup();
    std.debug.print("Storage engine initialized and recovered from WAL.\n", .{});

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    std.debug.print("Query engine initialized.\n", .{});

    const server_config = server.ServerConfig{
        .port = 8080,
        .enable_logging = true,
    };

    var kausaldb_server = server.KausalDBServer.init(allocator, server_config, &storage_engine, &query_eng);
    defer kausaldb_server.deinit();

    std.debug.print("Starting KausalDB TCP server on port {d}...\n", .{server_config.port});

    try kausaldb_server.startup();
}

fn run_demo(allocator: std.mem.Allocator) !void {
    std.debug.print("=== KausalDB Storage and Query Demo ===\n\n", .{});
    log.info("Starting KausalDB demo with scoped logging", .{});

    var prod_vfs = production_vfs.ProductionVFS.init(allocator);
    const vfs_interface = prod_vfs.vfs();
    const cwd = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(cwd);
    const data_dir = try std.fs.path.join(allocator, &[_][]const u8{ cwd, "demo_data" });
    defer allocator.free(data_dir);

    // Ensure data directory exists before initializing storage engine
    vfs_interface.mkdir_all(data_dir) catch |err| switch (err) {
        vfs.VFSError.FileExists => {}, // Directory already exists, continue
        else => return err,
    };

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.startup();
    std.debug.print("✓ Storage engine initialized and recovered from WAL\n", .{});
    log.info("Storage engine startup completed successfully", .{});

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();
    std.debug.print("✓ Query engine initialized\n\n", .{});
    log.info("Query engine initialization completed", .{});

    const block1_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const block1 = ContextBlock{
        .id = block1_id,
        .version = 1,
        .source_uri = "git://github.com/example/repo.git/src/main.zig#L1-25",
        .metadata_json = "{\"type\":\"function\",\"language\":\"zig\",\"name\":\"main\"}",
        .content =
        \\pub fn main() !void {
        \\    const allocator = std.heap.page_allocator;
        \\    std.debug.print("Hello, KausalDB!\\n", .{});
        \\}
        ,
    };

    const block2_id = try BlockId.from_hex("fedcba9876543210123456789abcdef0");
    const block2 = ContextBlock{
        .id = block2_id,
        .version = 1,
        .source_uri = "git://github.com/example/repo.git/src/utils.zig#L10-20",
        .metadata_json = "{\"type\":\"function\",\"language\":\"zig\",\"name\":\"calculate_hash\"}",
        .content =
        \\pub fn calculate_hash(data: []const u8) u64 {
        \\    var hasher = std.hash.Wyhash.init(0);
        \\    hasher.update(data);
        \\    return hasher.final();
        \\}
        ,
    };

    std.debug.print("Storing context blocks...\n", .{});
    try storage_engine.put_block(block1);
    try storage_engine.put_block(block2);

    const stats = query_eng.statistics();
    std.debug.print("✓ Stored {} blocks\n\n", .{stats.total_blocks_stored});

    std.debug.print("Querying single block by ID...\n", .{});
    var single_result = try query_eng.find_block(block1_id);
    defer single_result.deinit();

    if (single_result.total_found > 0) {
        if (try single_result.next()) |block| {
            defer single_result.deinit_block(block);
            std.debug.print("✓ Found block: {s}\n", .{block.source_uri});
        }
    }

    std.debug.print("\nQuerying multiple blocks...\n", .{});
    const query = query_engine.FindBlocksQuery{
        .block_ids = &[_]BlockId{ block1_id, block2_id },
    };

    var multi_result = try query_eng.execute_find_blocks(query);
    defer multi_result.deinit();

    std.debug.print("✓ Found {} blocks\n\n", .{multi_result.total_found});

    std.debug.print("Formatting results for LLM consumption:\n", .{});
    std.debug.print("=====================================\n", .{});

    var formatted_output = std.ArrayList(u8).init(allocator);
    defer formatted_output.deinit();
    try multi_result.format_for_llm(formatted_output.writer().any());

    std.debug.print("{s}", .{formatted_output.items});
    std.debug.print("=====================================\n\n", .{});

    const metrics = storage_engine.metrics();
    std.debug.print("\n=== Storage Metrics ===\n", .{});
    std.debug.print("Blocks: {} written, {} read, {} deleted\n", .{
        metrics.blocks_written.load(),
        metrics.blocks_read.load(),
        metrics.blocks_deleted.load(),
    });
    std.debug.print("WAL: {} writes, {} flushes, {} recoveries\n", .{
        metrics.wal_writes.load(),
        metrics.wal_flushes.load(),
        metrics.wal_recoveries.load(),
    });
    std.debug.print("Latency: {} ns write, {} ns read\n", .{
        metrics.average_write_latency_ns(),
        metrics.average_read_latency_ns(),
    });

    const query_stats = query_eng.statistics();
    std.debug.print("\n=== Query Engine Metrics ===\n", .{});
    std.debug.print("Storage: {} blocks available\n", .{query_stats.total_blocks_stored});
    std.debug.print("Queries: {} total ({} find_blocks, {} traversal)\n", .{
        query_stats.queries_executed,
        query_stats.find_blocks_queries,
        query_stats.traversal_queries,
    });

    std.debug.print("\nDemo completed successfully!\n", .{});
    log.info("KausalDB demo completed successfully with scoped logging", .{});
}

test "main module tests" {
    // Tests for main module
}
