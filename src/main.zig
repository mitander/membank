//! Main entry point and CLI interface.

const std = @import("std");

const concurrency = @import("core/concurrency.zig");
const context_block = @import("core/types.zig");
const assert_mod = @import("core/assert.zig");
const production_vfs = @import("core/production_vfs.zig");
const query_engine = @import("query/engine.zig");
const server = @import("server/handler.zig");
const signals = @import("core/signals.zig");
const storage_mod = @import("storage/engine.zig");
const vfs = @import("core/vfs.zig");

const assert = assert_mod.assert;
const log = std.log.scoped(.main);

const BlockId = context_block.BlockId;
const ContextBlock = context_block.ContextBlock;
const QueryEngine = query_engine.QueryEngine;
const StorageEngine = storage_mod.StorageEngine;

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
    var data_dir: []const u8 = "kausaldb_data";
    var port: u16 = 8080;
    var host: []const u8 = "127.0.0.1";

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--help")) {
            std.debug.print(
                \\Usage: kausaldb server [options]
                \\
                \\Options:
                \\  --data-dir <path>   Specify data directory (default: kausaldb_data)
                \\  --port <number>     Server port (default: 8080)
                \\  --host <address>    Bind address (default: 127.0.0.1, use 0.0.0.0 for all interfaces)
                \\  --help              Show this help message
                \\
                \\Security Notice:
                \\  KausalDB v0.1.0 does not include authentication or encryption.
                \\  Only run on trusted networks. Use --host 127.0.0.1 (default) for
                \\  localhost-only access, or --host 0.0.0.0 for all interfaces.
                \\
                \\Monitoring:
                \\  Use 'kausaldb status --format prometheus' for Prometheus metrics
                \\
            , .{});
            return;
        } else if (std.mem.eql(u8, args[i], "--data-dir") and i + 1 < args.len) {
            data_dir = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--port") and i + 1 < args.len) {
            port = std.fmt.parseInt(u16, args[i + 1], 10) catch {
                std.debug.print("Error: Invalid port value\n", .{});
                std.process.exit(1);
            };
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--host") and i + 1 < args.len) {
            host = args[i + 1];
            i += 1;
        } else {
            std.debug.print("Unknown server option: {s}\n", .{args[i]});
            std.debug.print("Use 'kausaldb server --help' for usage information\n", .{});
            std.process.exit(1);
        }
    }

    try signals.setup_signal_handlers();

    var prod_vfs = production_vfs.ProductionVFS.init(allocator);
    const vfs_interface = prod_vfs.vfs();

    std.debug.print("KausalDB server starting...\n", .{});

    const cwd = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(cwd);
    const full_data_dir = try std.fs.path.join(allocator, &[_][]const u8{ cwd, data_dir });
    defer allocator.free(full_data_dir);

    vfs_interface.mkdir_all(full_data_dir) catch |err| switch (err) {
        vfs.VFSError.FileExists => {}, // Directory already exists, continue
        else => {
            std.debug.print("Failed to create data directory '{s}': {}\n", .{ full_data_dir, err });
            return err;
        },
    };

    var storage_engine = try StorageEngine.init_default(allocator, vfs_interface, full_data_dir);
    defer storage_engine.deinit();

    try storage_engine.startup();
    std.debug.print("Storage engine initialized and recovered from WAL.\n", .{});

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    std.debug.print("Query engine initialized.\n", .{});

    const server_config = server.ServerConfig{
        .port = port,
        .host = host,
        .enable_logging = true,
    };

    var kausal_server = server.Server.init(allocator, server_config, &storage_engine, &query_eng);
    defer kausal_server.deinit();

    std.debug.print("Starting KausalDB TCP server on {s}:{d}...\n", .{ server_config.host, server_config.port });
    std.debug.print("Data directory: {s}\n", .{full_data_dir});
    std.debug.print("Press Ctrl+C to shutdown gracefully\n", .{});

    try kausal_server.startup();

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

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--data-dir") and i + 1 < args.len) {
            data_dir = args[i + 1];
            i += 1; // Skip the next argument
        } else if (std.mem.eql(u8, args[i], "--format") and i + 1 < args.len) {
            const format = args[i + 1];
            if (std.mem.eql(u8, format, "prometheus")) {
                try run_status_prometheus(allocator, data_dir);
                return;
            } else {
                std.debug.print("Error: Unknown format '{s}'. Supported: prometheus\n", .{format});
                std.process.exit(1);
            }
        } else if (std.mem.eql(u8, args[i], "--help")) {
            std.debug.print(
                \\Usage: kausaldb status [options]
                \\
                \\Options:
                \\  --data-dir <path>   Specify data directory (default: kausaldb_data)
                \\  --format <format>   Output format: human (default) or prometheus
                \\  --help              Show this help message
                \\
                \\Examples:
                \\  kausaldb status                           # Human-readable status
                \\  kausaldb status --format prometheus       # Prometheus metrics format
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

    if (!vfs_interface.exists(full_data_dir)) {
        std.debug.print("Status: Not initialized (data directory does not exist)\n", .{});
        std.debug.print("\nTo initialize: kausaldb server\n", .{});
        return;
    }

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

    std.debug.print("Use 'kausaldb query --id <block_id>' to view specific blocks\n", .{});
    std.debug.print("Listing functionality requires storage engine iteration support\n", .{});
}

fn run_query(allocator: std.mem.Allocator, args: [][:0]u8) !void {
    var data_dir: []const u8 = "kausaldb_data";
    var query_id: ?[]const u8 = null;

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

fn run_status_prometheus(allocator: std.mem.Allocator, data_dir: []const u8) !void {
    var prod_vfs = production_vfs.ProductionVFS.init(allocator);
    const vfs_interface = prod_vfs.vfs();

    const cwd = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(cwd);
    const full_data_dir = try std.fs.path.join(allocator, &[_][]const u8{ cwd, data_dir });
    defer allocator.free(full_data_dir);

    if (!vfs_interface.exists(full_data_dir)) {
        std.debug.print("# KausalDB metrics - database not initialized\n", .{});
        std.debug.print("kausaldb_up 0\n", .{});
        return;
    }

    var storage_engine = StorageEngine.init_default(allocator, vfs_interface, full_data_dir) catch {
        std.debug.print("# KausalDB metrics - database error\n", .{});
        std.debug.print("kausaldb_up 0\n", .{});
        return;
    };
    defer storage_engine.deinit();

    storage_engine.startup() catch {
        std.debug.print("# KausalDB metrics - startup failed\n", .{});
        std.debug.print("kausaldb_up 0\n", .{});
        return;
    };

    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();
    query_eng.startup();

    const storage_metrics = storage_engine.metrics();
    const query_stats = query_eng.statistics();

    std.debug.print("# HELP kausaldb_up Whether KausalDB is up and responding\n", .{});
    std.debug.print("# TYPE kausaldb_up gauge\n", .{});
    std.debug.print("kausaldb_up 1\n", .{});

    std.debug.print("# HELP kausaldb_blocks_written_total Total number of blocks written to storage\n", .{});
    std.debug.print("# TYPE kausaldb_blocks_written_total counter\n", .{});
    std.debug.print("kausaldb_blocks_written_total {}\n", .{storage_metrics.blocks_written.load()});

    std.debug.print("# HELP kausaldb_blocks_read_total Total number of blocks read from storage\n", .{});
    std.debug.print("# TYPE kausaldb_blocks_read_total counter\n", .{});
    std.debug.print("kausaldb_blocks_read_total {}\n", .{storage_metrics.blocks_read.load()});

    std.debug.print("# HELP kausaldb_blocks_deleted_total Total number of blocks deleted from storage\n", .{});
    std.debug.print("# TYPE kausaldb_blocks_deleted_total counter\n", .{});
    std.debug.print("kausaldb_blocks_deleted_total {}\n", .{storage_metrics.blocks_deleted.load()});

    std.debug.print("# HELP kausaldb_wal_writes_total Total number of WAL writes\n", .{});
    std.debug.print("# TYPE kausaldb_wal_writes_total counter\n", .{});
    std.debug.print("kausaldb_wal_writes_total {}\n", .{storage_metrics.wal_writes.load()});

    std.debug.print("# HELP kausaldb_wal_flushes_total Total number of WAL flushes\n", .{});
    std.debug.print("# TYPE kausaldb_wal_flushes_total counter\n", .{});
    std.debug.print("kausaldb_wal_flushes_total {}\n", .{storage_metrics.wal_flushes.load()});

    std.debug.print("# HELP kausaldb_wal_recoveries_total Total number of WAL recoveries\n", .{});
    std.debug.print("# TYPE kausaldb_wal_recoveries_total counter\n", .{});
    std.debug.print("kausaldb_wal_recoveries_total {}\n", .{storage_metrics.wal_recoveries.load()});

    std.debug.print("# HELP kausaldb_write_latency_nanoseconds_avg Average write latency in nanoseconds\n", .{});
    std.debug.print("# TYPE kausaldb_write_latency_nanoseconds_avg gauge\n", .{});
    std.debug.print("kausaldb_write_latency_nanoseconds_avg {}\n", .{storage_metrics.average_write_latency_ns()});

    std.debug.print("# HELP kausaldb_read_latency_nanoseconds_avg Average read latency in nanoseconds\n", .{});
    std.debug.print("# TYPE kausaldb_read_latency_nanoseconds_avg gauge\n", .{});
    std.debug.print("kausaldb_read_latency_nanoseconds_avg {}\n", .{storage_metrics.average_read_latency_ns()});

    std.debug.print("# HELP kausaldb_total_blocks_stored Total number of blocks currently stored\n", .{});
    std.debug.print("# TYPE kausaldb_total_blocks_stored gauge\n", .{});
    std.debug.print("kausaldb_total_blocks_stored {}\n", .{query_stats.total_blocks_stored});

    std.debug.print("# HELP kausaldb_queries_executed_total Total number of queries executed\n", .{});
    std.debug.print("# TYPE kausaldb_queries_executed_total counter\n", .{});
    std.debug.print("kausaldb_queries_executed_total {}\n", .{query_stats.queries_executed});

    std.debug.print("# HELP kausaldb_find_blocks_queries_total Total number of find_blocks queries\n", .{});
    std.debug.print("# TYPE kausaldb_find_blocks_queries_total counter\n", .{});
    std.debug.print("kausaldb_find_blocks_queries_total {}\n", .{query_stats.find_blocks_queries});

    std.debug.print("# HELP kausaldb_traversal_queries_total Total number of traversal queries\n", .{});
    std.debug.print("# TYPE kausaldb_traversal_queries_total counter\n", .{});
    std.debug.print("kausaldb_traversal_queries_total {}\n", .{query_stats.traversal_queries});
}

test "main module tests" {}
