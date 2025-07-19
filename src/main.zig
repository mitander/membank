//! CortexDB main entry point and CLI interface.

const std = @import("std");
const assert = std.debug.assert;
const storage = @import("storage");
const query_engine = @import("query_engine");
const context_block = @import("context_block");
const simulation = @import("simulation");
const simulation_vfs = @import("simulation_vfs");
const concurrency = @import("concurrency");

const StorageEngine = storage.StorageEngine;
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
    std.debug.print("CortexDB v0.1.0\n", .{});
}

fn print_usage() !void {
    std.debug.print(
        \\CortexDB - High-performance context database
        \\
        \\Usage:
        \\  cortexdb <command> [options]
        \\
        \\Commands:
        \\  version    Show version information
        \\  help       Show this help message
        \\  server     Start the database server
        \\  demo       Run a storage and query demonstration
        \\
        \\Examples:
        \\  cortexdb server --port 8080
        \\  cortexdb demo
        \\  cortexdb version
        \\
    , .{});
}

fn run_server(allocator: std.mem.Allocator, args: [][:0]u8) !void {
    _ = args;

    std.debug.print("CortexDB server starting...\n", .{});

    // Create simulation VFS for demonstration
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "cortexdb_data");

    // Initialize storage engine
    var storage_engine = StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();
    try storage_engine.recover_from_wal();
    std.debug.print("Storage engine initialized and recovered from WAL.\n", .{});

    // Initialize query engine
    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();

    std.debug.print("Query engine initialized.\n", .{});
    std.debug.print("Server ready! (Use Ctrl+C to stop)\n", .{});

    // Simple server loop (placeholder)
    while (true) {
        std.Thread.sleep(1000000000); // Sleep for 1 second
    }
}

fn run_demo(allocator: std.mem.Allocator) !void {
    std.debug.print("=== CortexDB Storage and Query Demo ===\n\n", .{});

    // Create simulation VFS
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "demo_data");

    // Initialize storage engine
    var storage_engine = StorageEngine.init(allocator, vfs_interface, data_dir);
    defer storage_engine.deinit();

    try storage_engine.initialize_storage();
    try storage_engine.recover_from_wal();
    std.debug.print("✓ Storage engine initialized and recovered from WAL\n", .{});

    // Initialize query engine
    var query_eng = QueryEngine.init(allocator, &storage_engine);
    defer query_eng.deinit();
    std.debug.print("✓ Query engine initialized\n\n", .{});

    // Create sample context blocks
    const block1_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const block1 = ContextBlock{
        .id = block1_id,
        .version = 1,
        .source_uri = "git://github.com/example/repo.git/src/main.zig#L1-25",
        .metadata_json = "{\"type\":\"function\",\"language\":\"zig\",\"name\":\"main\"}",
        .content =
        \\pub fn main() !void {
        \\    const allocator = std.heap.page_allocator;
        \\    std.debug.print("Hello, CortexDB!\\n", .{});
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

    // Store blocks
    std.debug.print("Storing context blocks...\n", .{});
    try storage_engine.put_block(block1);
    try storage_engine.put_block(block2);

    const stats = query_eng.get_statistics();
    std.debug.print("✓ Stored {} blocks\n\n", .{stats.total_blocks_stored});

    // Query single block
    std.debug.print("Querying single block by ID...\n", .{});
    const single_result = try query_eng.find_block_by_id(block1_id);
    defer single_result.deinit();

    if (single_result.count > 0) {
        std.debug.print("✓ Found block: {s}\n", .{single_result.blocks[0].source_uri});
    }

    // Query multiple blocks
    std.debug.print("\nQuerying multiple blocks...\n", .{});
    const query = query_engine.GetBlocksQuery{
        .block_ids = &[_]BlockId{ block1_id, block2_id },
    };

    const multi_result = try query_eng.execute_get_blocks(query);
    defer multi_result.deinit();

    std.debug.print("✓ Found {} blocks\n\n", .{multi_result.count});

    // Format for LLM
    std.debug.print("Formatting results for LLM consumption:\n", .{});
    std.debug.print("=====================================\n", .{});
    const formatted = try multi_result.format_for_llm(allocator);
    defer allocator.free(formatted);

    std.debug.print("{s}", .{formatted});
    std.debug.print("=====================================\n\n", .{});

    std.debug.print("Demo completed successfully!\n", .{});
}

test "main module tests" {
    // Tests for main module
}
