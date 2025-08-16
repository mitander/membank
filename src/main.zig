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
const signals = @import("core/signals.zig");
const server = @import("server/handler.zig");

const StorageEngine = storage_mod.StorageEngine;
const QueryEngine = query_engine.QueryEngine;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

pub fn main() !void {
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
    } else if (std.mem.eql(u8, command, "status")) {
        try run_status(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "list-blocks")) {
        try run_list_blocks(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "query")) {
        try run_query(allocator, args[2..]);
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
        \\  version      Show version information
        \\  help         Show this help message
        \\  server       Start the database server
        \\  demo         Run a storage and query demonstration
        \\  status       Show database status and statistics
        \\  list-blocks  List stored context blocks
        \\  query        Query blocks by ID or content
        \\
        \\Examples:
        \\  kausaldb server --port 8080
        \\  kausaldb demo
        \\  kausaldb version
        \\  kausaldb status --data-dir ./my_data
        \\  kausaldb list-blocks --limit 10
        \\  kausaldb query --id 0123456789abcdef...
        \\
    , .{});
}

fn run_server(allocator: std.mem.Allocator, args: [][:0]u8) !void {
    _ = args;

    // Setup signal handlers for graceful shutdown
    try signals.setup_signal_handlers();

    var prod_vfs = production_vfs.ProductionVFS.init(allocator);
    const vfs_interface = prod_vfs.vfs();

    std.debug.print("KausalDB server starting...\n", .{});

    const cwd = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(cwd);
    const data_dir = try std.fs.path.join(allocator, &[_][]const u8{ cwd, "kausaldb_data" });
    defer allocator.free(data_dir);

    // Ensure data directory exists before initializing storage engine
    vfs_interface.mkdir_all(data_dir) catch |err| switch (err) {
        vfs.VFSError.FileExists => {}, // Directory already exists, continue
        else => {
            std.debug.print("Failed to create data directory '{s}': {}\n", .{ data_dir, err });
            return err;
        },
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

    var kausal_server = server.Server.init(allocator, server_config, &storage_engine, &query_eng);
    defer kausal_server.deinit();

    std.debug.print("Starting KausalDB TCP server on port {d}...\n", .{server_config.port});
    std.debug.print("Press Ctrl+C to shutdown gracefully\n", .{});

    try kausal_server.startup();

    // Server has exited gracefully
    std.debug.print("KausalDB server shutdown complete\n", .{});
}

fn run_demo(allocator: std.mem.Allocator) !void {
    var prod_vfs = production_vfs.ProductionVFS.init(allocator);
    const vfs_interface = prod_vfs.vfs();

    std.debug.print("=== KausalDB Storage and Query Demo ===\n\n", .{});
    log.info("Starting KausalDB demo with scoped logging", .{});

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
        \\fn calculate_hash(data: []const u8) u64 {
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
    const maybe_block = try query_eng.find_block(block1_id);

    if (maybe_block) |block| {
        std.debug.print("✓ Found block: {s}\n", .{block.block.source_uri});
    }

    std.debug.print("\nQuerying multiple blocks...\n", .{});
    const block_ids = &[_]BlockId{ block1_id, block2_id };

    var found_blocks = std.ArrayList(ContextBlock).init(allocator);
    try found_blocks.ensureTotalCapacity(block_ids.len);
    defer found_blocks.deinit();

    for (block_ids) |block_id| {
        if (try query_eng.find_block(block_id)) |block| {
            try found_blocks.append(block.block);
        }
    }

    std.debug.print("✓ Found {} blocks\n\n", .{found_blocks.items.len});

    std.debug.print("Formatting results for LLM consumption:\n", .{});
    std.debug.print("=====================================\n", .{});

    for (found_blocks.items, 0..) |block, i| {
        std.debug.print("--- BEGIN CONTEXT BLOCK ---\n", .{});
        std.debug.print("Block {} (ID: {any}):\n", .{ i + 1, block.id });
        std.debug.print("Source: {s}\n", .{block.source_uri});
        std.debug.print("Version: {}\n", .{block.version});
        std.debug.print("Metadata: {s}\n", .{block.metadata_json});
        std.debug.print("Content: {s}\n", .{block.content});
        std.debug.print("--- END CONTEXT BLOCK ---\n\n", .{});
    }

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

fn run_status(allocator: std.mem.Allocator, args: [][:0]u8) !void {
    var data_dir: []const u8 = "kausaldb_data";

    // Parse command line arguments
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--data-dir") and i + 1 < args.len) {
            data_dir = args[i + 1];
            i += 1; // Skip the next argument
        } else if (std.mem.eql(u8, args[i], "--help")) {
            std.debug.print(
                \\Usage: kausaldb status [options]
                \\
                \\Options:
                \\  --data-dir <path>  Specify data directory (default: kausaldb_data)
                \\  --help             Show this help message
                \\
            , .{});
            return;
        }
    }

    var prod_vfs = production_vfs.ProductionVFS.init(allocator);
    const vfs_interface = prod_vfs.vfs();

    const cwd = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(cwd);
    const full_data_dir = try std.fs.path.join(allocator, &[_][]const u8{ cwd, data_dir });
    defer allocator.free(full_data_dir);

    std.debug.print("=== KausalDB Status ===\n\n", .{});
    std.debug.print("Data directory: {s}\n", .{full_data_dir});

    // Check if data directory exists
    if (!vfs_interface.exists(full_data_dir)) {
        std.debug.print("Status: Not initialized (data directory does not exist)\n", .{});
        std.debug.print("\nTo initialize: kausaldb server\n", .{});
        return;
    }

    // Try to initialize storage engine in read-only mode
    var storage_engine = StorageEngine.init_default(allocator, vfs_interface, full_data_dir) catch |err| {
        std.debug.print("Status: Database error - {any}\n", .{err});
        return;
    };
    defer storage_engine.deinit();

    storage_engine.startup() catch |err| {
        std.debug.print("Status: Startup failed - {any}\n", .{err});
        return;
    };

    std.debug.print("Status: Healthy\n\n", .{});

    const metrics = storage_engine.metrics();
    std.debug.print("=== Storage Metrics ===\n", .{});
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
}

fn run_list_blocks(allocator: std.mem.Allocator, args: [][:0]u8) !void {
    var data_dir: []const u8 = "kausaldb_data";
    var limit: u32 = 10;

    // Parse command line arguments
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--data-dir") and i + 1 < args.len) {
            data_dir = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--limit") and i + 1 < args.len) {
            limit = std.fmt.parseInt(u32, args[i + 1], 10) catch {
                std.debug.print("Error: Invalid limit value\n", .{});
                return;
            };
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--help")) {
            std.debug.print(
                \\Usage: kausaldb list-blocks [options]
                \\
                \\Options:
                \\  --data-dir <path>  Specify data directory (default: kausaldb_data)
                \\  --limit <number>   Maximum blocks to display (default: 10)
                \\  --help             Show this help message
                \\
            , .{});
            return;
        }
    }

    var prod_vfs = production_vfs.ProductionVFS.init(allocator);
    const vfs_interface = prod_vfs.vfs();

    const cwd = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(cwd);
    const full_data_dir = try std.fs.path.join(allocator, &[_][]const u8{ cwd, data_dir });
    defer allocator.free(full_data_dir);

    var storage_engine = StorageEngine.init_default(allocator, vfs_interface, full_data_dir) catch |err| {
        std.debug.print("Error: Cannot access database - {any}\n", .{err});
        std.debug.print("Run 'kausaldb status' to check database health\n", .{});
        return;
    };
    defer storage_engine.deinit();

    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    query_eng.startup();

    const stats = query_eng.statistics();
    std.debug.print("=== Context Blocks ({} total) ===\n\n", .{stats.total_blocks_stored});

    if (stats.total_blocks_stored == 0) {
        std.debug.print("No blocks stored in database\n", .{});
        std.debug.print("\nTo add blocks: kausaldb demo\n", .{});
        return;
    }

    // For this simple implementation, we'll just show the summary
    // A full implementation would need iterator support in the storage engine
    std.debug.print("Use 'kausaldb query --id <block_id>' to view specific blocks\n", .{});
    std.debug.print("Listing functionality requires storage engine iteration support\n", .{});
}

fn run_query(allocator: std.mem.Allocator, args: [][:0]u8) !void {
    var data_dir: []const u8 = "kausaldb_data";
    var query_id: ?[]const u8 = null;

    // Parse command line arguments
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--data-dir") and i + 1 < args.len) {
            data_dir = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--id") and i + 1 < args.len) {
            query_id = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--help")) {
            std.debug.print(
                \\Usage: kausaldb query [options]
                \\
                \\Options:
                \\  --data-dir <path>  Specify data directory (default: kausaldb_data)
                \\  --id <block_id>    Query block by hex ID (32 character hex string)
                \\  --help             Show this help message
                \\
                \\Examples:
                \\  kausaldb query --id 0123456789abcdeffedcba9876543210
                \\
            , .{});
            return;
        }
    }

    if (query_id == null) {
        std.debug.print("Error: --id parameter is required\n", .{});
        std.debug.print("Use 'kausaldb query --help' for usage information\n", .{});
        return;
    }

    const block_id = BlockId.from_hex(query_id.?) catch |err| {
        std.debug.print("Error: Invalid block ID format - {any}\n", .{err});
        std.debug.print("Block ID must be a 32-character hex string\n", .{});
        return;
    };

    var prod_vfs = production_vfs.ProductionVFS.init(allocator);
    const vfs_interface = prod_vfs.vfs();

    const cwd = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(cwd);
    const full_data_dir = try std.fs.path.join(allocator, &[_][]const u8{ cwd, data_dir });
    defer allocator.free(full_data_dir);

    var storage_engine = StorageEngine.init_default(allocator, vfs_interface, full_data_dir) catch |err| {
        std.debug.print("Error: Cannot access database - {any}\n", .{err});
        return;
    };
    defer storage_engine.deinit();

    try storage_engine.startup();

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    query_eng.startup();

    const maybe_block = query_eng.find_block(block_id) catch |err| {
        std.debug.print("Error: Query failed - {any}\n", .{err});
        return;
    };

    if (maybe_block) |block| {
        const block_data = block.read(.query_engine);
        std.debug.print("=== Context Block Found ===\n\n", .{});
        std.debug.print("ID: {any}\n", .{block_data.id});
        std.debug.print("Version: {}\n", .{block_data.version});
        std.debug.print("Source: {s}\n", .{block_data.source_uri});
        std.debug.print("Metadata: {s}\n", .{block_data.metadata_json});
        std.debug.print("\nContent:\n{s}\n", .{block_data.content});
    } else {
        std.debug.print("Block not found: {any}\n", .{block_id});
        std.debug.print("\nUse 'kausaldb list-blocks' to see available blocks\n", .{});
    }
}

test "main module tests" {
    // Tests for main module
}
