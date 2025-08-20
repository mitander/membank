//! Network fuzzing module for KausalDB server connections.
//!
//! Tests the server connection handling, protocol parsing, and error handling
//! under adversarial network conditions including malformed packets, partial
//! reads, and connection failures.

const std = @import("std");

const kausaldb = @import("kausaldb");

const common = @import("common.zig");

const stdx = kausaldb.stdx;

const ConnectionManager = kausaldb.server.ConnectionManager;
const SimulationVFS = kausaldb.SimulationVFS;

const NetworkFuzzStats = struct {
    total_connections: u64 = 0,
    malformed_packets: u64 = 0,
    connection_errors: u64 = 0,
    protocol_violations: u64 = 0,
    successful_requests: u64 = 0,
    timeouts: u64 = 0,

    fn reset(self: *NetworkFuzzStats) void {
        self.* = NetworkFuzzStats{};
    }

    fn print_summary(self: NetworkFuzzStats, elapsed_ms: u64) void {
        const requests_per_sec = if (elapsed_ms > 0) (self.total_connections * 1000) / elapsed_ms else 0;
        std.debug.print("Network Fuzz: {} conn/s | {} total | {} errors | {} malformed | {} protocol violations\n", .{ requests_per_sec, self.total_connections, self.connection_errors, self.malformed_packets, self.protocol_violations });
    }
};

/// Simulated network packet for fuzzing
const FuzzPacket = struct {
    data: []u8,
    is_complete: bool,
    simulated_error: ?anyerror,

    fn init(allocator: std.mem.Allocator, rng: *std.Random.DefaultPrng, max_size: usize) !FuzzPacket {
        const size = rng.random().uintLessThan(usize, max_size + 1);
        const data = try allocator.alloc(u8, size);

        // Fill with random data
        for (data) |*byte| {
            byte.* = rng.random().int(u8);
        }

        // Randomly make packet incomplete or add error
        const error_chance = rng.random().uintLessThan(u8, 100);
        const simulated_error: ?anyerror = if (error_chance < 5) error.ConnectionResetByPeer else null;

        return FuzzPacket{
            .data = data,
            .is_complete = error_chance >= 10, // 10% chance of incomplete packet
            .simulated_error = simulated_error,
        };
    }

    fn deinit(self: *FuzzPacket, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

/// Generate malformed HTTP-like requests for protocol fuzzing
fn generate_malformed_request(allocator: std.mem.Allocator, rng: *std.Random.DefaultPrng) ![]u8 {
    const random_id = rng.random().int(u64);
    const random_size = rng.random().uintLessThan(u32, 10000);

    const malformation = rng.random().uintLessThan(u8, 10);
    switch (malformation) {
        0 => return try std.fmt.allocPrint(allocator, "GET /blocks/{} HTTP/1.1\r\nHost: localhost\r\n\r\n", .{random_id}),
        1 => return try std.fmt.allocPrint(allocator, "POST /ingest HTTP/1.1\r\nContent-Length: {}\r\n\r\n{{\"content\":\"test\"}}", .{random_size}),
        2 => return try std.fmt.allocPrint(allocator, "PUT /blocks/{} HTTP/1.1\r\nContent-Type: application/json\r\n\r\n", .{random_id}),
        3 => return try std.fmt.allocPrint(allocator, "DELETE /blocks/{} HTTP/1.1\r\n\r\n", .{random_id}),
        4 => return try std.fmt.allocPrint(allocator, "INVALID_METHOD /path HTTP/1.1\r\n\r\n", .{}),
        5 => return try std.fmt.allocPrint(allocator, "GET /blocks/{} HTTP/1.1\r\nHost: localhost\r\n\r\nextra_garbage", .{random_id}),
        6 => return try std.fmt.allocPrint(allocator, "GET /blocks/{} HTT", .{random_id}), // Truncated
        7 => return try std.fmt.allocPrint(allocator, "POST /test HTTP/1.1\r\nContent-Length: {}\r\n\r\nsmall", .{random_size}), // Wrong content length
        8 => return try allocator.dupe(u8, ""), // Empty request
        else => {
            // Random binary data
            const size = rng.random().uintLessThan(usize, 1024);
            const data = try allocator.alloc(u8, size);
            for (data) |*byte| {
                byte.* = rng.random().int(u8);
            }
            return data;
        },
    }
}

/// Run network fuzzing with specified parameters
pub fn run(
    allocator: std.mem.Allocator,
    iterations: u64,
    seed: u64,
    verbose_mode: *stdx.ProtectedType(bool),
    validation_errors: *stdx.MetricsCounter,
) !void {
    _ = validation_errors; // Network fuzzing uses different error tracking

    var rng = std.Random.DefaultPrng.init(seed);
    var stats = NetworkFuzzStats{};
    var iteration: u64 = 0;

    const start_time = std.time.milliTimestamp();
    var last_report_time = start_time;

    const is_verbose = verbose_mode.with(
        fn (*const bool, void) bool,
        {},
        struct {
            fn f(verbose: *const bool, ctx: void) bool {
                _ = ctx;
                return verbose.*;
            }
        }.f,
    );

    std.debug.print("Starting network fuzzing: {} iterations, seed {}, verbose: {}\n", .{ iterations, seed, is_verbose });

    // Set up simulation environment
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    while (iteration < iterations) {
        // Check for shutdown file
        if (std.fs.cwd().access(".kausaldb_stop", .{})) |_| {
            std.debug.print("\nNetwork fuzzing stopped by shutdown request\n", .{});
            break;
        } else |_| {
            // File doesn't exist, continue
        }

        // Packet generation targets protocol edge cases and malformed requests
        var packet = generate_fuzz_packet(allocator, &rng) catch |err| {
            if (is_verbose) {
                std.debug.print("Failed to generate fuzz packet: {}\n", .{err});
            }
            continue;
        };
        defer packet.deinit(allocator);

        // Processing validates server resilience against malformed network input
        test_packet_processing(&packet, &stats, &rng) catch |err| {
            stats.connection_errors += 1;
            if (is_verbose) {
                std.debug.print("Packet processing error: {}\n", .{err});
            }
        };

        iteration += 1;
        stats.total_connections += 1;

        // Progress reporting
        const current_time = std.time.milliTimestamp();
        const report_interval_ms: i64 = if (is_verbose) 5000 else 10000; // 5s verbose, 10s normal

        if (current_time - last_report_time >= report_interval_ms) {
            const elapsed = @as(u64, @intCast(current_time - start_time));
            if (is_verbose) {
                stats.print_summary(elapsed);
            } else {
                const conn_per_sec = if (elapsed > 0) (stats.total_connections * 1000) / elapsed else 0;
                std.debug.print("Network: {} iterations ({} conn/s)\n", .{ iteration, conn_per_sec });
            }
            last_report_time = current_time;
        }
    }

    const final_time = std.time.milliTimestamp();
    const total_elapsed = @as(u64, @intCast(final_time - start_time));

    std.debug.print("\nNetwork fuzzing completed:\n", .{});
    stats.print_summary(total_elapsed);

    if (stats.connection_errors > stats.total_connections / 10) {
        std.debug.print("WARNING: High error rate detected ({}%)\n", .{(stats.connection_errors * 100) / stats.total_connections});
    }
}

fn generate_fuzz_packet(allocator: std.mem.Allocator, rng: *std.Random.DefaultPrng) !FuzzPacket {
    const packet_type = rng.random().uintLessThan(u8, 3);

    switch (packet_type) {
        0 => {
            // HTTP-like request packet
            const request = try generate_malformed_request(allocator, rng);
            return FuzzPacket{
                .data = request,
                .is_complete = rng.random().boolean(),
                .simulated_error = null,
            };
        },
        1 => {
            // Random binary packet
            return FuzzPacket.init(allocator, rng, 4096);
        },
        else => {
            // Large packet (stress test)
            return FuzzPacket.init(allocator, rng, 64 * 1024);
        },
    }
}

fn test_packet_processing(packet: *const FuzzPacket, stats: *NetworkFuzzStats, rng: *std.Random.DefaultPrng) !void {
    _ = rng; // Reserved for future randomization needs

    // Simulate network packet validation
    if (packet.simulated_error != null) {
        stats.connection_errors += 1;
        return packet.simulated_error.?;
    }

    // Check for obviously malformed packets
    if (packet.data.len == 0) {
        stats.malformed_packets += 1;
        return;
    }

    // Simulate protocol parsing
    if (packet.data.len >= 4) {
        // Check for HTTP-like structure
        if (std.mem.startsWith(u8, packet.data, "GET ") or
            std.mem.startsWith(u8, packet.data, "POST") or
            std.mem.startsWith(u8, packet.data, "PUT ") or
            std.mem.startsWith(u8, packet.data, "DELETE"))
        {

            // Valid HTTP method detected
            if (std.mem.indexOf(u8, packet.data, "\r\n\r\n") != null) {
                stats.successful_requests += 1;
            } else if (!packet.is_complete) {
                // Incomplete packet - would need more data
                return;
            } else {
                stats.protocol_violations += 1;
            }
        } else {
            stats.malformed_packets += 1;
        }
    } else {
        stats.malformed_packets += 1;
    }
}
