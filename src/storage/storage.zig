//! Storage engine for CortexDB Context Block persistence.
//!
//! Implements a Log-Structured Merge-Tree (LSMT) with Write-Ahead Logging
//! for durable, high-performance storage of Context Blocks and Graph Edges.
//! All I/O operations go through the VFS interface for deterministic testing.

const std = @import("std");
const custom_assert = @import("assert");
const assert = custom_assert.assert;
const assert_not_null = custom_assert.assert_not_null;
const assert_not_empty = custom_assert.assert_not_empty;
const assert_state_valid = custom_assert.assert_state_valid;
const comptime_assert = custom_assert.comptime_assert;
const log = std.log.scoped(.storage);
const vfs = @import("vfs");
const context_block = @import("context_block");
const sstable = @import("sstable");
const error_context = @import("error_context");
const concurrency = @import("concurrency");
const tiered_compaction = @import("tiered_compaction");

const VFS = vfs.VFS;
const ContextBlock = context_block.ContextBlock;
const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;
const EdgeType = context_block.EdgeType;
const SSTable = sstable.SSTable;
const Compactor = sstable.Compactor;
const TieredCompactionManager = tiered_compaction.TieredCompactionManager;

/// Maximum size of a WAL segment before rotation (64MB).
/// Chosen to balance between recovery time and file handle usage.
const MAX_WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Default maximum memory size for memtable before flushing to SSTable (128MB).
/// This prevents unpredictable memory usage and potential OOM crashes with large blocks.
const DEFAULT_MEMTABLE_MAX_SIZE: u64 = 128 * 1024 * 1024;

// Compile-time guarantees for architectural invariants
comptime {
    comptime_assert(MAX_WAL_SEGMENT_SIZE & (MAX_WAL_SEGMENT_SIZE - 1) == 0, "MAX_WAL_SEGMENT_SIZE must be a power of two for efficient alignment and bitwise operations");
    comptime_assert(DEFAULT_MEMTABLE_MAX_SIZE >= 64 * 1024 * 1024, "Memtable max size must be at least 64MB to avoid excessive flushes");
}

/// Configuration options for the storage engine.
pub const Config = struct {
    /// Maximum memory size for memtable before flushing to SSTable.
    /// Prevents unpredictable memory usage and OOM crashes with large blocks.
    memtable_max_size: u64 = DEFAULT_MEMTABLE_MAX_SIZE,

    pub fn validate(self: Config) !void {
        // Allow smaller sizes for testing (minimum 1MB), but recommend 16MB+ for production
        if (self.memtable_max_size < 1024 * 1024) {
            return error.MemtableMaxSizeTooSmall;
        }
        if (self.memtable_max_size > 1024 * 1024 * 1024) {
            return error.MemtableMaxSizeTooLarge;
        }
    }
};

/// Performance metrics for storage engine observability.
/// All counters are atomic for thread-safe access in concurrent environments.
pub const StorageMetrics = struct {
    // Block operations
    blocks_written: std.atomic.Value(u64),
    blocks_read: std.atomic.Value(u64),
    blocks_deleted: std.atomic.Value(u64),

    // WAL operations
    wal_writes: std.atomic.Value(u64),
    wal_flushes: std.atomic.Value(u64),
    wal_recoveries: std.atomic.Value(u64),

    // SSTable operations
    sstable_reads: std.atomic.Value(u64),
    sstable_writes: std.atomic.Value(u64),
    compactions: std.atomic.Value(u64),

    // Edge operations
    edges_added: std.atomic.Value(u64),
    edges_removed: std.atomic.Value(u64),

    // Performance timings (in nanoseconds)
    total_write_time_ns: std.atomic.Value(u64),
    total_read_time_ns: std.atomic.Value(u64),
    total_wal_flush_time_ns: std.atomic.Value(u64),

    // Error counts
    write_errors: std.atomic.Value(u64),
    read_errors: std.atomic.Value(u64),
    wal_errors: std.atomic.Value(u64),

    // Storage utilization metrics
    total_bytes_written: std.atomic.Value(u64),
    total_bytes_read: std.atomic.Value(u64),
    wal_bytes_written: std.atomic.Value(u64),
    sstable_bytes_written: std.atomic.Value(u64),

    pub fn init() StorageMetrics {
        return StorageMetrics{
            .blocks_written = std.atomic.Value(u64).init(0),
            .blocks_read = std.atomic.Value(u64).init(0),
            .blocks_deleted = std.atomic.Value(u64).init(0),
            .wal_writes = std.atomic.Value(u64).init(0),
            .wal_flushes = std.atomic.Value(u64).init(0),
            .wal_recoveries = std.atomic.Value(u64).init(0),
            .sstable_reads = std.atomic.Value(u64).init(0),
            .sstable_writes = std.atomic.Value(u64).init(0),
            .compactions = std.atomic.Value(u64).init(0),
            .edges_added = std.atomic.Value(u64).init(0),
            .edges_removed = std.atomic.Value(u64).init(0),
            .total_write_time_ns = std.atomic.Value(u64).init(0),
            .total_read_time_ns = std.atomic.Value(u64).init(0),
            .total_wal_flush_time_ns = std.atomic.Value(u64).init(0),
            .write_errors = std.atomic.Value(u64).init(0),
            .read_errors = std.atomic.Value(u64).init(0),
            .wal_errors = std.atomic.Value(u64).init(0),
            .total_bytes_written = std.atomic.Value(u64).init(0),
            .total_bytes_read = std.atomic.Value(u64).init(0),
            .wal_bytes_written = std.atomic.Value(u64).init(0),
            .sstable_bytes_written = std.atomic.Value(u64).init(0),
        };
    }

    /// Get average write latency in nanoseconds.
    pub fn average_write_latency_ns(self: *const StorageMetrics) u64 {
        const writes = self.blocks_written.load(.monotonic);
        if (writes == 0) return 0;
        return self.total_write_time_ns.load(.monotonic) / writes;
    }

    /// Get average read latency in nanoseconds.
    pub fn average_read_latency_ns(self: *const StorageMetrics) u64 {
        const reads = self.blocks_read.load(.monotonic);
        if (reads == 0) return 0;
        return self.total_read_time_ns.load(.monotonic) / reads;
    }

    /// Get average WAL flush latency in nanoseconds.
    pub fn average_wal_flush_latency_ns(self: *const StorageMetrics) u64 {
        const flushes = self.wal_flushes.load(.monotonic);
        if (flushes == 0) return 0;
        return self.total_wal_flush_time_ns.load(.monotonic) / flushes;
    }

    /// Get average bytes per block written.
    pub fn average_block_size_bytes(self: *const StorageMetrics) u64 {
        const blocks = self.blocks_written.load(.monotonic);
        if (blocks == 0) return 0;
        return self.total_bytes_written.load(.monotonic) / blocks;
    }

    /// Get storage write throughput in bytes per second.
    pub fn write_throughput_bps(self: *const StorageMetrics) f64 {
        const total_time_ns = self.total_write_time_ns.load(.monotonic);
        const total_time_seconds = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0;
        if (total_time_seconds == 0.0) return 0.0;
        const total_bytes = self.total_bytes_written.load(.monotonic);
        return @as(f64, @floatFromInt(total_bytes)) / total_time_seconds;
    }

    /// Get storage read throughput in bytes per second.
    pub fn read_throughput_bps(self: *const StorageMetrics) f64 {
        const total_time_ns = self.total_read_time_ns.load(.monotonic);
        const total_time_seconds = @as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0;
        if (total_time_seconds == 0.0) return 0.0;
        const total_bytes = self.total_bytes_read.load(.monotonic);
        return @as(f64, @floatFromInt(total_bytes)) / total_time_seconds;
    }

    /// Format metrics as human-readable text.
    pub fn format_human_readable(self: *const StorageMetrics, writer: anytype) !void {
        try writer.writeAll("=== Storage Metrics ===\n");
        try writer.print("Blocks: {} written, {} read, {} deleted\n", .{
            self.blocks_written.load(.monotonic),
            self.blocks_read.load(.monotonic),
            self.blocks_deleted.load(.monotonic),
        });
        try writer.print("WAL: {} writes, {} flushes, {} recoveries\n", .{
            self.wal_writes.load(.monotonic),
            self.wal_flushes.load(.monotonic),
            self.wal_recoveries.load(.monotonic),
        });
        try writer.print("SSTable: {} reads, {} writes, {} compactions\n", .{
            self.sstable_reads.load(.monotonic),
            self.sstable_writes.load(.monotonic),
            self.compactions.load(.monotonic),
        });
        try writer.print("Edges: {} added, {} removed\n", .{
            self.edges_added.load(.monotonic),
            self.edges_removed.load(.monotonic),
        });
        try writer.print("Latency: {} ns write, {} ns read, {} ns WAL flush\n", .{
            self.average_write_latency_ns(),
            self.average_read_latency_ns(),
            self.average_wal_flush_latency_ns(),
        });
        try writer.print("Throughput: {d:.2} MB/s write, {d:.2} MB/s read\n", .{
            self.write_throughput_bps() / (1024.0 * 1024.0),
            self.read_throughput_bps() / (1024.0 * 1024.0),
        });
        try writer.print("Data: {d:.2} MB written, {d:.2} MB read, avg block {d:.2} KB\n", .{
            @as(f64, @floatFromInt(self.total_bytes_written.load(.monotonic))) / (1024.0 * 1024.0),
            @as(f64, @floatFromInt(self.total_bytes_read.load(.monotonic))) / (1024.0 * 1024.0),
            @as(f64, @floatFromInt(self.average_block_size_bytes())) / 1024.0,
        });
        try writer.print("Errors: {} write, {} read, {} WAL\n", .{
            self.write_errors.load(.monotonic),
            self.read_errors.load(.monotonic),
            self.wal_errors.load(.monotonic),
        });
    }

    /// Format metrics as JSON for programmatic consumption.
    pub fn format_json(self: *const StorageMetrics, writer: anytype) !void {
        try writer.writeAll("{\n");
        try writer.print("  \"blocks_written\": {},\n", .{self.blocks_written.load(.monotonic)});
        try writer.print("  \"blocks_read\": {},\n", .{self.blocks_read.load(.monotonic)});
        try writer.print("  \"blocks_deleted\": {},\n", .{self.blocks_deleted.load(.monotonic)});
        try writer.print("  \"wal_writes\": {},\n", .{self.wal_writes.load(.monotonic)});
        try writer.print("  \"wal_flushes\": {},\n", .{self.wal_flushes.load(.monotonic)});
        try writer.print("  \"wal_recoveries\": {},\n", .{self.wal_recoveries.load(.monotonic)});
        try writer.print("  \"sstable_reads\": {},\n", .{self.sstable_reads.load(.monotonic)});
        try writer.print("  \"sstable_writes\": {},\n", .{self.sstable_writes.load(.monotonic)});
        try writer.print("  \"compactions\": {},\n", .{self.compactions.load(.monotonic)});
        try writer.print("  \"edges_added\": {},\n", .{self.edges_added.load(.monotonic)});
        try writer.print("  \"edges_removed\": {},\n", .{self.edges_removed.load(.monotonic)});
        try writer.print(
            "  \"total_bytes_written\": {},\n",
            .{self.total_bytes_written.load(.monotonic)},
        );
        try writer.print(
            "  \"total_bytes_read\": {},\n",
            .{self.total_bytes_read.load(.monotonic)},
        );
        try writer.print(
            "  \"wal_bytes_written\": {},\n",
            .{self.wal_bytes_written.load(.monotonic)},
        );
        try writer.print(
            "  \"sstable_bytes_written\": {},\n",
            .{self.sstable_bytes_written.load(.monotonic)},
        );
        try writer.print(
            "  \"average_write_latency_ns\": {},\n",
            .{self.average_write_latency_ns()},
        );
        try writer.print(
            "  \"average_read_latency_ns\": {},\n",
            .{self.average_read_latency_ns()},
        );
        try writer.print(
            "  \"average_wal_flush_latency_ns\": {},\n",
            .{self.average_wal_flush_latency_ns()},
        );
        try writer.print(
            "  \"average_block_size_bytes\": {},\n",
            .{self.average_block_size_bytes()},
        );
        try writer.print("  \"write_throughput_bps\": {d:.2},\n", .{self.write_throughput_bps()});
        try writer.print("  \"read_throughput_bps\": {d:.2},\n", .{self.read_throughput_bps()});
        try writer.print("  \"write_errors\": {},\n", .{self.write_errors.load(.monotonic)});
        try writer.print("  \"read_errors\": {},\n", .{self.read_errors.load(.monotonic)});
        try writer.print("  \"wal_errors\": {}\n", .{self.wal_errors.load(.monotonic)});
        try writer.writeAll("}\n");
    }
};

/// Storage engine errors.
pub const StorageError = error{
    /// Block not found in storage
    BlockNotFound,
    /// Corrupted WAL entry
    CorruptedWALEntry,
    /// Invalid checksum
    InvalidChecksum,
    /// Storage already initialized
    AlreadyInitialized,
    /// Storage not initialized
    NotInitialized,
    /// WAL file corrupted
    WALCorrupted,
    /// Memtable max size too small (must be at least 1MB)
    MemtableMaxSizeTooSmall,
    /// Memtable max size too large (must be at most 1GB)
    MemtableMaxSizeTooLarge,
} || std.mem.Allocator.Error || anyerror;

/// WAL entry types as defined in the data model specification.
const WALEntryType = enum(u8) {
    put_block = 0x01,
    delete_block = 0x02,
    put_edge = 0x03,

    pub fn from_u8(value: u8) !WALEntryType {
        return std.meta.intToEnum(WALEntryType, value) catch error.InvalidWALEntryType;
    }
};

/// WAL entry header structure.
pub const WALEntry = struct {
    checksum: u64,
    entry_type: WALEntryType,
    payload_size: u32,
    payload: []const u8,

    pub const HEADER_SIZE = 13; // 8 bytes checksum + 1 byte type + 4 bytes payload_size

    /// Calculate CRC-64 checksum of type and payload.
    fn calculate_checksum(entry_type: WALEntryType, payload: []const u8) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(&[_]u8{@intFromEnum(entry_type)});
        hasher.update(payload);
        return hasher.final();
    }

    /// Serialize WAL entry to buffer.
    pub fn serialize(self: WALEntry, buffer: []u8) !usize {
        const total_size = HEADER_SIZE + self.payload.len;
        if (buffer.len < total_size) return error.BufferTooSmall;

        var offset: usize = 0;

        // Write checksum (8 bytes, little-endian)
        std.mem.writeInt(u64, buffer[offset..][0..8], self.checksum, .little);
        offset += 8;

        // Write entry type (1 byte)
        buffer[offset] = @intFromEnum(self.entry_type);
        offset += 1;

        // Write payload size (4 bytes, little-endian)
        std.mem.writeInt(u32, buffer[offset..][0..4], self.payload_size, .little);
        offset += 4;

        // Write payload
        @memcpy(buffer[offset .. offset + self.payload.len], self.payload);
        offset += self.payload.len;

        return offset;
    }

    /// Deserialize WAL entry from buffer.
    pub fn deserialize(buffer: []const u8, allocator: std.mem.Allocator) !WALEntry {
        if (buffer.len < HEADER_SIZE) return error.BufferTooSmall;

        var offset: usize = 0;

        // Read checksum
        const checksum = std.mem.readInt(u64, buffer[offset..][0..8], .little);
        offset += 8;

        // Read entry type
        const entry_type = try WALEntryType.from_u8(buffer[offset]);
        offset += 1;

        // Read payload size
        const payload_size = std.mem.readInt(u32, buffer[offset..][0..4], .little);
        offset += 4;

        // Validate payload size against remaining buffer
        if (offset + payload_size > buffer.len) return error.BufferTooSmall;

        // Read payload
        const payload = try allocator.dupe(u8, buffer[offset .. offset + payload_size]);

        // Verify checksum
        const expected_checksum = calculate_checksum(entry_type, payload);
        if (checksum != expected_checksum) {
            allocator.free(payload);
            return StorageError.InvalidChecksum;
        }

        return WALEntry{
            .checksum = checksum,
            .entry_type = entry_type,
            .payload_size = payload_size,
            .payload = payload,
        };
    }

    /// Create WAL entry for putting a Context Block.
    pub fn create_put_block(block: ContextBlock, allocator: std.mem.Allocator) !WALEntry {
        const payload_size = block.serialized_size();

        // Debug the serialization issue in ReleaseSafe mode
        if (payload_size == 0) {
            std.debug.panic("ContextBlock serialized_size returned 0: source_uri.len={}, metadata_json.len={}, content.len={}", .{ block.source_uri.len, block.metadata_json.len, block.content.len });
        }

        const payload = try allocator.alloc(u8, payload_size);

        // More detailed debugging
        if (payload.len == 0 and payload_size > 0) {
            std.debug.panic("Allocator returned empty slice for size {}: possible allocator corruption", .{payload_size});
        }

        const bytes_written = try block.serialize(payload);

        if (bytes_written != payload_size) {
            std.debug.panic("Serialization size mismatch: expected {}, got {}", .{ payload_size, bytes_written });
        }

        const checksum = calculate_checksum(.put_block, payload);

        return WALEntry{
            .checksum = checksum,
            .entry_type = .put_block,
            .payload_size = @intCast(payload_size),
            .payload = payload,
        };
    }

    /// Create WAL entry for deleting a Context Block.
    pub fn create_delete_block(block_id: BlockId, allocator: std.mem.Allocator) !WALEntry {
        const payload = try allocator.dupe(u8, &block_id.bytes);
        const checksum = calculate_checksum(.delete_block, payload);

        return WALEntry{
            .checksum = checksum,
            .entry_type = .delete_block,
            .payload_size = @intCast(payload.len),
            .payload = payload,
        };
    }

    /// Create WAL entry for putting a Graph Edge.
    pub fn create_put_edge(edge: GraphEdge, allocator: std.mem.Allocator) !WALEntry {
        const payload = try allocator.alloc(u8, 40); // GraphEdge.SERIALIZED_SIZE
        _ = try edge.serialize(payload);

        const checksum = calculate_checksum(.put_edge, payload);

        return WALEntry{
            .checksum = checksum,
            .entry_type = .put_edge,
            .payload_size = @intCast(payload.len),
            .payload = payload,
        };
    }

    /// Free allocated payload memory.
    pub fn deinit(self: WALEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
    }
};
// In-memory block index for fast lookups.
//
// ## Memory Management: The Arena-per-Subsystem Model
//
// The BlockIndex employs a two-part memory strategy for performance and safety, which
// is a core architectural pattern in CortexDB.
//
// - `backing_allocator`: A stable, long-lived allocator (e.g., GeneralPurposeAllocator)
//   is used for the HashMap structure itself. This part of the index persists.
//
// - `arena`: An ArenaAllocator is used for ALL data stored within the HashMap. This includes
//   every `source_uri`, `metadata_json`, and `content` string.
//
// The rationale for this design is the `flush_memtable_to_sstable` operation. Instead of
// iterating through thousands of blocks to free each string individually, the entire
// set of in-memory blocks can be deallocated in a single, O(1) operation by calling
// `arena.reset()`. This is fundamental to achieving high ingestion throughput.
pub const BlockIndex = struct {
    blocks: std.HashMap(
        BlockId,
        ContextBlock,
        BlockIdContext,
        std.hash_map.default_max_load_percentage,
    ),
    arena: std.heap.ArenaAllocator,
    backing_allocator: std.mem.Allocator,
    /// Track total memory used by blocks in arena (strings only).
    /// Does not include HashMap overhead, just the content bytes.
    memory_used: u64,

    pub const BlockIdContext = struct {
        pub fn hash(self: @This(), block_id: BlockId) u64 {
            _ = self;
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(&block_id.bytes);
            return hasher.final();
        }

        pub fn eql(self: @This(), a: BlockId, b: BlockId) bool {
            _ = self;
            return a.eql(b);
        }
    };

    pub fn init(allocator: std.mem.Allocator) BlockIndex {
        const arena = std.heap.ArenaAllocator.init(allocator);

        return BlockIndex{
            .blocks = std.HashMap(
                BlockId,
                ContextBlock,
                BlockIdContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator), // HashMap uses stable backing allocator
            .arena = arena,
            .backing_allocator = allocator,
            .memory_used = 0,
        };
    }

    pub fn deinit(self: *BlockIndex) void {
        // Clear HashMap first to avoid use-after-free of arena-allocated strings
        self.blocks.clearAndFree();
        self.arena.deinit();
    }

    pub fn put_block(self: *BlockIndex, block: ContextBlock) !void {
        assert(@intFromPtr(self) != 0, "BlockIndex self pointer cannot be null", .{});
        assert(@intFromPtr(&self.arena) != 0, "BlockIndex arena pointer cannot be null", .{});

        const arena_allocator = self.arena.allocator();

        // Validate string lengths to prevent allocation of corrupted sizes
        assert(block.source_uri.len < 1024 * 1024, "source_uri too large: {} bytes", .{block.source_uri.len});
        assert(block.metadata_json.len < 1024 * 1024, "metadata_json too large: {} bytes", .{block.metadata_json.len});
        assert(block.content.len < 100 * 1024 * 1024, "content too large: {} bytes", .{block.content.len});

        // Catch null pointers masquerading as slices
        if (block.source_uri.len > 0) {
            assert(@intFromPtr(block.source_uri.ptr) != 0, "source_uri has null pointer with non-zero length", .{});
        }
        if (block.metadata_json.len > 0) {
            assert(@intFromPtr(block.metadata_json.ptr) != 0, "metadata_json has null pointer with non-zero length", .{});
        }
        if (block.content.len > 0) {
            assert(@intFromPtr(block.content.ptr) != 0, "content has null pointer with non-zero length", .{});
        }

        const cloned_block = ContextBlock{
            .id = block.id,
            .version = block.version,
            .source_uri = try arena_allocator.dupe(u8, block.source_uri),
            .metadata_json = try arena_allocator.dupe(u8, block.metadata_json),
            .content = try arena_allocator.dupe(u8, block.content),
        };

        if (block.source_uri.len > 0) {
            assert(@intFromPtr(cloned_block.source_uri.ptr) != @intFromPtr(block.source_uri.ptr), "source_uri was not properly duped by arena allocator", .{});
        }
        if (block.metadata_json.len > 0) {
            assert(@intFromPtr(cloned_block.metadata_json.ptr) != @intFromPtr(block.metadata_json.ptr), "metadata_json was not properly duped by arena allocator", .{});
        }
        if (block.content.len > 0) {
            assert(@intFromPtr(cloned_block.content.ptr) != @intFromPtr(block.content.ptr), "content was not properly duped by arena allocator", .{});
        }

        // Check if we're replacing an existing block to adjust memory accounting
        if (self.blocks.get(block.id)) |existing_block| {
            // Subtract old block's memory usage
            const old_memory = existing_block.source_uri.len + existing_block.metadata_json.len + existing_block.content.len;
            assert(self.memory_used >= old_memory, "Memory accounting corruption: {} < {}", .{ self.memory_used, old_memory });
            self.memory_used -= old_memory;
        }

        // Add new block's memory usage
        const new_memory = block.source_uri.len + block.metadata_json.len + block.content.len;
        self.memory_used += new_memory;

        try self.blocks.put(block.id, cloned_block);
    }

    pub fn find_block(self: *BlockIndex, block_id: BlockId) ?*const ContextBlock {
        return self.blocks.getPtr(block_id);
    }

    pub fn remove_block(self: *BlockIndex, block_id: BlockId) void {
        // Subtract removed block's memory usage
        if (self.blocks.get(block_id)) |existing_block| {
            const old_memory = existing_block.source_uri.len + existing_block.metadata_json.len + existing_block.content.len;
            assert(self.memory_used >= old_memory, "Memory accounting corruption: {} < {}", .{ self.memory_used, old_memory });
            self.memory_used -= old_memory;
        }
        _ = self.blocks.remove(block_id);
    }

    pub fn block_count(self: *const BlockIndex) u32 {
        return @intCast(self.blocks.count());
    }

    /// Get the current memory usage of blocks in the arena (strings only).
    pub fn memory_usage(self: *const BlockIndex) u64 {
        return self.memory_used;
    }

    pub fn clear(self: *BlockIndex) void {
        assert(@intFromPtr(self) != 0, "BlockIndex self pointer cannot be null during clear", .{});
        assert(@intFromPtr(&self.arena) != 0, "BlockIndex arena pointer cannot be null during clear", .{});

        self.blocks.clearRetainingCapacity();
        _ = self.arena.reset(.retain_capacity);
        self.memory_used = 0;

        assert(self.blocks.count() == 0, "HashMap not properly cleared", .{});
    }
};

/// In-memory graph edge index for fast graph traversal.
const GraphEdgeIndex = struct {
    /// Outgoing edges indexed by source_id
    outgoing_edges: std.HashMap(
        BlockId,
        std.ArrayList(GraphEdge),
        BlockIdContext,
        std.hash_map.default_max_load_percentage,
    ),
    /// Incoming edges indexed by target_id
    incoming_edges: std.HashMap(
        BlockId,
        std.ArrayList(GraphEdge),
        BlockIdContext,
        std.hash_map.default_max_load_percentage,
    ),
    arena: std.heap.ArenaAllocator,
    backing_allocator: std.mem.Allocator,

    const BlockIdContext = struct {
        pub fn hash(self: @This(), block_id: BlockId) u64 {
            _ = self;
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(&block_id.bytes);
            return hasher.final();
        }

        pub fn eql(self: @This(), a: BlockId, b: BlockId) bool {
            _ = self;
            return a.eql(b);
        }
    };

    pub fn init(allocator: std.mem.Allocator) GraphEdgeIndex {
        const arena = std.heap.ArenaAllocator.init(allocator);

        return GraphEdgeIndex{
            .outgoing_edges = std.HashMap(
                BlockId,
                std.ArrayList(GraphEdge),
                BlockIdContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator), // HashMap uses stable backing allocator
            .incoming_edges = std.HashMap(
                BlockId,
                std.ArrayList(GraphEdge),
                BlockIdContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator), // HashMap uses stable backing allocator
            .arena = arena,
            .backing_allocator = allocator,
        };
    }

    pub fn deinit(self: *GraphEdgeIndex) void {
        // Clean up ArrayLists then HashMap then arena
        var outgoing_iter = self.outgoing_edges.iterator();
        while (outgoing_iter.next()) |entry| {
            entry.value_ptr.deinit();
        }
        var incoming_iter = self.incoming_edges.iterator();
        while (incoming_iter.next()) |entry| {
            entry.value_ptr.deinit();
        }

        self.outgoing_edges.deinit();
        self.incoming_edges.deinit();
        self.arena.deinit();
    }

    pub fn put_edge(self: *GraphEdgeIndex, edge: GraphEdge) !void {
        const arena_allocator = self.arena.allocator();

        // Add to outgoing edges index
        var outgoing_result = try self.outgoing_edges.getOrPut(edge.source_id);
        if (!outgoing_result.found_existing) {
            outgoing_result.value_ptr.* = std.ArrayList(GraphEdge).init(arena_allocator);
        }
        try outgoing_result.value_ptr.append(edge);

        // Add to incoming edges index
        var incoming_result = try self.incoming_edges.getOrPut(edge.target_id);
        if (!incoming_result.found_existing) {
            incoming_result.value_ptr.* = std.ArrayList(GraphEdge).init(arena_allocator);
        }
        try incoming_result.value_ptr.append(edge);
    }

    /// Find outgoing edges from a source block.
    pub fn find_outgoing_edges(self: *const GraphEdgeIndex, source_id: BlockId) ?[]const GraphEdge {
        if (self.outgoing_edges.getPtr(source_id)) |edge_list| {
            return edge_list.items;
        }
        return null;
    }

    /// Find incoming edges to a target block.
    pub fn find_incoming_edges(self: *const GraphEdgeIndex, target_id: BlockId) ?[]const GraphEdge {
        if (self.incoming_edges.getPtr(target_id)) |edge_list| {
            return edge_list.items;
        }
        return null;
    }

    /// Remove all edges involving a specific block (when block is deleted).
    pub fn remove_block_edges(self: *GraphEdgeIndex, block_id: BlockId) void {
        // Remove outgoing edges
        if (self.outgoing_edges.fetchRemove(block_id)) |kv| {
            kv.value.deinit();
        }

        // Remove incoming edges
        if (self.incoming_edges.fetchRemove(block_id)) |kv| {
            kv.value.deinit();
        }
    }

    /// Get total edge count.
    pub fn edge_count(self: *const GraphEdgeIndex) u32 {
        var total: u32 = 0;
        var iterator = self.outgoing_edges.iterator();
        while (iterator.next()) |entry| {
            total += @intCast(entry.value_ptr.items.len);
        }
        return total;
    }

    /// Traversal options for graph operations.
    pub const TraversalOptions = struct {
        max_depth: ?u32 = null,
        max_results: ?u32 = null,
        edge_types: ?[]const EdgeType = null, // Filter by edge types
    };

    /// Traversal result containing visited nodes and path information.
    pub const TraversalResult = struct {
        visited_blocks: std.ArrayList(BlockId),
        paths: std.ArrayList(std.ArrayList(BlockId)), // Paths from start to each visited block
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator) TraversalResult {
            return TraversalResult{
                .visited_blocks = std.ArrayList(BlockId).init(allocator),
                .paths = std.ArrayList(std.ArrayList(BlockId)).init(allocator),
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *TraversalResult) void {
            for (self.paths.items) |path| {
                path.deinit();
            }
            self.paths.deinit();
            self.visited_blocks.deinit();
        }
    };

    /// Breadth-First Search traversal following outgoing edges.
    pub fn traverse_outgoing(
        self: *const GraphEdgeIndex,
        start_id: BlockId,
        options: TraversalOptions,
        allocator: std.mem.Allocator,
    ) !TraversalResult {
        var result = TraversalResult.init(allocator);
        var visited = std.HashMap(
            BlockId,
            void,
            BlockIdContext,
            std.hash_map.default_max_load_percentage,
        ).init(allocator);
        defer visited.deinit();

        // BFS queue: (block_id, depth, path_to_block)
        const QueueItem = struct {
            block_id: BlockId,
            depth: u32,
            path: std.ArrayList(BlockId),
        };
        var queue = std.ArrayList(QueueItem).init(allocator);
        defer {
            for (queue.items) |item| {
                item.path.deinit();
            }
            queue.deinit();
        }

        // Initialize with start block
        var start_path = std.ArrayList(BlockId).init(allocator);
        try start_path.append(start_id);
        try queue.append(QueueItem{
            .block_id = start_id,
            .depth = 0,
            .path = start_path,
        });
        try visited.put(start_id, {});

        var queue_index: usize = 0;
        while (queue_index < queue.items.len) {
            const current = queue.items[queue_index];
            queue_index += 1;

            // Add to results
            try result.visited_blocks.append(current.block_id);
            try result.paths.append(try current.path.clone());

            // Check limits
            if (options.max_results) |max_results| {
                if (result.visited_blocks.items.len >= max_results) break;
            }

            // Check depth limit
            if (options.max_depth) |max_depth| {
                if (current.depth >= max_depth) continue;
            }

            // Follow outgoing edges
            if (self.find_outgoing_edges(current.block_id)) |edges| {
                for (edges) |edge| {
                    // Filter by edge types if specified
                    if (options.edge_types) |edge_types| {
                        var type_matches = false;
                        for (edge_types) |edge_type| {
                            if (edge.edge_type == edge_type) {
                                type_matches = true;
                                break;
                            }
                        }
                        if (!type_matches) continue;
                    }

                    // Skip if already visited
                    if (visited.contains(edge.target_id)) continue;

                    // Add to queue
                    var new_path = try current.path.clone();
                    try new_path.append(edge.target_id);
                    try queue.append(QueueItem{
                        .block_id = edge.target_id,
                        .depth = current.depth + 1,
                        .path = new_path,
                    });
                    try visited.put(edge.target_id, {});
                }
            }
        }

        return result;
    }

    /// Breadth-First Search traversal following incoming edges.
    pub fn traverse_incoming(
        self: *const GraphEdgeIndex,
        start_id: BlockId,
        options: TraversalOptions,
        allocator: std.mem.Allocator,
    ) !TraversalResult {
        var result = TraversalResult.init(allocator);
        var visited = std.HashMap(
            BlockId,
            void,
            BlockIdContext,
            std.hash_map.default_max_load_percentage,
        ).init(allocator);
        defer visited.deinit();

        const QueueItem = struct {
            block_id: BlockId,
            depth: u32,
            path: std.ArrayList(BlockId),
        };
        var queue = std.ArrayList(QueueItem).init(allocator);
        defer {
            for (queue.items) |item| {
                item.path.deinit();
            }
            queue.deinit();
        }

        // Initialize with start block
        var start_path = std.ArrayList(BlockId).init(allocator);
        try start_path.append(start_id);
        try queue.append(QueueItem{
            .block_id = start_id,
            .depth = 0,
            .path = start_path,
        });
        try visited.put(start_id, {});

        var queue_index: usize = 0;
        while (queue_index < queue.items.len) {
            const current = queue.items[queue_index];
            queue_index += 1;

            // Add to results
            try result.visited_blocks.append(current.block_id);
            try result.paths.append(try current.path.clone());

            // Check limits
            if (options.max_results) |max_results| {
                if (result.visited_blocks.items.len >= max_results) break;
            }

            // Check depth limit
            if (options.max_depth) |max_depth| {
                if (current.depth >= max_depth) continue;
            }

            // Follow incoming edges
            if (self.find_incoming_edges(current.block_id)) |edges| {
                for (edges) |edge| {
                    // Filter by edge types if specified
                    if (options.edge_types) |edge_types| {
                        var type_matches = false;
                        for (edge_types) |edge_type| {
                            if (edge.edge_type == edge_type) {
                                type_matches = true;
                                break;
                            }
                        }
                        if (!type_matches) continue;
                    }

                    // Skip if already visited
                    if (visited.contains(edge.source_id)) continue;

                    // Add to queue
                    var new_path = try current.path.clone();
                    try new_path.append(edge.source_id);
                    try queue.append(QueueItem{
                        .block_id = edge.source_id,
                        .depth = current.depth + 1,
                        .path = new_path,
                    });
                    try visited.put(edge.source_id, {});
                }
            }
        }

        return result;
    }

    /// Bidirectional traversal following both incoming and outgoing edges.
    pub fn traverse_bidirectional(
        self: *const GraphEdgeIndex,
        start_id: BlockId,
        options: TraversalOptions,
        allocator: std.mem.Allocator,
    ) !TraversalResult {
        var result = TraversalResult.init(allocator);
        var visited = std.HashMap(
            BlockId,
            void,
            BlockIdContext,
            std.hash_map.default_max_load_percentage,
        ).init(allocator);
        defer visited.deinit();

        const QueueItem = struct {
            block_id: BlockId,
            depth: u32,
            path: std.ArrayList(BlockId),
        };
        var queue = std.ArrayList(QueueItem).init(allocator);
        defer {
            for (queue.items) |item| {
                item.path.deinit();
            }
            queue.deinit();
        }

        // Initialize with start block
        var start_path = std.ArrayList(BlockId).init(allocator);
        try start_path.append(start_id);
        try queue.append(QueueItem{
            .block_id = start_id,
            .depth = 0,
            .path = start_path,
        });
        try visited.put(start_id, {});

        var queue_index: usize = 0;
        while (queue_index < queue.items.len) {
            const current = queue.items[queue_index];
            queue_index += 1;

            // Add to results
            try result.visited_blocks.append(current.block_id);
            try result.paths.append(try current.path.clone());

            // Check limits
            if (options.max_results) |max_results| {
                if (result.visited_blocks.items.len >= max_results) break;
            }

            // Check depth limit
            if (options.max_depth) |max_depth| {
                if (current.depth >= max_depth) continue;
            }

            // Follow outgoing edges
            if (self.find_outgoing_edges(current.block_id)) |edges| {
                for (edges) |edge| {
                    if (self.should_follow_edge(edge, options) and
                        !visited.contains(edge.target_id))
                    {
                        var new_path = try current.path.clone();
                        try new_path.append(edge.target_id);
                        try queue.append(QueueItem{
                            .block_id = edge.target_id,
                            .depth = current.depth + 1,
                            .path = new_path,
                        });
                        try visited.put(edge.target_id, {});
                    }
                }
            }

            // Follow incoming edges
            if (self.find_incoming_edges(current.block_id)) |edges| {
                for (edges) |edge| {
                    if (self.should_follow_edge(edge, options) and
                        !visited.contains(edge.source_id))
                    {
                        var new_path = try current.path.clone();
                        try new_path.append(edge.source_id);
                        try queue.append(QueueItem{
                            .block_id = edge.source_id,
                            .depth = current.depth + 1,
                            .path = new_path,
                        });
                        try visited.put(edge.source_id, {});
                    }
                }
            }
        }

        return result;
    }

    /// Helper function to check if an edge should be followed based on options.
    fn should_follow_edge(
        self: *const GraphEdgeIndex,
        edge: GraphEdge,
        options: TraversalOptions,
    ) bool {
        _ = self;
        if (options.edge_types) |edge_types| {
            for (edge_types) |edge_type| {
                if (edge.edge_type == edge_type) return true;
            }
            return false;
        }
        return true;
    }
};

/// Storage engine state.
pub const StorageEngine = struct {
    backing_allocator: std.mem.Allocator,
    vfs: VFS,
    data_dir: []const u8,
    config: Config,
    index: BlockIndex,
    graph_index: GraphEdgeIndex,
    wal_file: ?vfs.VFile,
    wal_segment_number: u32,
    wal_segment_size: u64,
    sstables: std.ArrayList([]const u8), // Paths to SSTable files
    compaction_manager: TieredCompactionManager,
    next_sstable_id: u32,
    initialized: bool,
    storage_metrics: StorageMetrics,

    /// Initialize a new storage engine instance with default configuration.
    pub fn init_default(
        allocator: std.mem.Allocator,
        filesystem: VFS,
        data_dir: []const u8,
    ) !StorageEngine {
        return init(allocator, filesystem, data_dir, Config{});
    }

    /// Initialize a new storage engine instance.
    pub fn init(
        allocator: std.mem.Allocator,
        filesystem: VFS,
        data_dir: []const u8,
        config: Config,
    ) !StorageEngine {
        // Validate configuration
        try config.validate();

        // Clone data_dir with backing allocator for simplicity
        const owned_data_dir = try allocator.dupe(u8, data_dir);

        const engine = StorageEngine{
            .backing_allocator = allocator,
            .vfs = filesystem,
            .data_dir = owned_data_dir,
            .config = config,
            .index = BlockIndex.init(allocator),
            .graph_index = GraphEdgeIndex.init(allocator),
            .wal_file = null,
            .wal_segment_number = 0,
            .wal_segment_size = 0,
            .sstables = std.ArrayList([]const u8).init(allocator),
            .compaction_manager = TieredCompactionManager.init(
                allocator,
                filesystem,
                owned_data_dir,
            ),
            .next_sstable_id = 0,
            .initialized = false,
            .storage_metrics = StorageMetrics.init(),
        };

        return engine;
    }

    /// Clean up storage engine resources.
    pub fn deinit(self: *StorageEngine) void {
        concurrency.assert_main_thread();
        if (self.wal_file) |*file| {
            file.close() catch {};
        }

        // Clean up SSTable paths
        for (self.sstables.items) |sstable_path| {
            self.backing_allocator.free(sstable_path);
        }
        self.sstables.deinit();

        // Clean up indexes
        self.index.deinit();
        self.graph_index.deinit();

        // Clean up compaction manager
        self.compaction_manager.deinit();

        // Clean up data_dir
        self.backing_allocator.free(self.data_dir);
    }

    /// Initialize storage engine by creating necessary directories and files.
    pub fn initialize_storage(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
        assert(!self.initialized, "StorageEngine is already initialized", .{});
        if (self.initialized) return StorageError.AlreadyInitialized;

        // Create data directory structure
        if (!self.vfs.exists(self.data_dir)) {
            try self.vfs.mkdir(self.data_dir);
        }

        const wal_dir = try std.fmt.allocPrint(self.backing_allocator, "{s}/wal", .{self.data_dir});
        defer self.backing_allocator.free(wal_dir);
        if (!self.vfs.exists(wal_dir)) {
            try self.vfs.mkdir(wal_dir);
        }

        const sst_dir = try std.fmt.allocPrint(self.backing_allocator, "{s}/sst", .{self.data_dir});
        defer self.backing_allocator.free(sst_dir);
        if (!self.vfs.exists(sst_dir)) {
            try self.vfs.mkdir(sst_dir);
        }

        // Find the highest existing WAL segment number
        const existing_segments = try self.list_wal_files(wal_dir);
        defer {
            for (existing_segments) |segment| {
                self.backing_allocator.free(segment);
            }
            self.backing_allocator.free(existing_segments);
        }

        if (existing_segments.len > 0) {
            // Extract segment number from last file (wal_XXXX.log)
            const last_segment = existing_segments[existing_segments.len - 1];
            if (last_segment.len >= 12 and std.mem.startsWith(u8, last_segment, "wal_")) {
                const num_str = last_segment[4..8];
                self.wal_segment_number = std.fmt.parseInt(u32, num_str, 10) catch 0;
            }
        }

        // Open or create current WAL segment
        const wal_path = try std.fmt.allocPrint(
            self.backing_allocator,
            "{s}/wal_{d:0>4}.log",
            .{ wal_dir, self.wal_segment_number },
        );
        defer self.backing_allocator.free(wal_path);

        // Open existing WAL file for writing, or create new one if it doesn't exist
        // This allows WAL recovery to work correctly
        if (self.vfs.exists(wal_path)) {
            self.wal_file = try self.vfs.open(wal_path, .write);
            // Seek to end for append behavior
            const end_pos = try self.wal_file.?.seek(0, .end);
            self.wal_segment_size = @intCast(end_pos);
        } else {
            self.wal_file = try self.vfs.create(wal_path);
            self.wal_segment_size = 0;
        }

        // Create LOCK file to prevent multiple instances
        const lock_path = try std.fmt.allocPrint(
            self.backing_allocator,
            "{s}/LOCK",
            .{self.data_dir},
        );
        defer self.backing_allocator.free(lock_path);

        var lock_file = try self.vfs.create(lock_path);
        defer lock_file.close() catch {};

        const process_id = 12345; // Placeholder PID for simulation
        const lock_content = try std.fmt.allocPrint(
            self.backing_allocator,
            "PID:{}\n",
            .{process_id},
        );
        defer self.backing_allocator.free(lock_content);

        _ = try lock_file.write(lock_content);

        // Discover and register existing SSTables with compaction manager
        try self.discover_existing_sstables();

        self.initialized = true;
    }

    /// Startup storage engine by initializing and recovering from WAL.
    pub fn startup(self: *StorageEngine) !void {
        try self.initialize_storage();
        try self.recover_from_wal();
    }

    /// Put a Context Block into storage.
    pub fn put_block(self: *StorageEngine, block: ContextBlock) !void {
        concurrency.assert_main_thread();

        assert(@intFromPtr(self) != 0, "StorageEngine self pointer cannot be null", .{});
        assert_state_valid(self.initialized, "StorageEngine must be initialized before put_block", .{});

        assert(block.version > 0, "ContextBlock version must be positive, got {}", .{block.version});

        if (!self.initialized) return StorageError.NotInitialized;

        const start_time = std.time.nanoTimestamp();

        // Track the operation attempt
        _ = self.storage_metrics.wal_writes.fetchAdd(1, .monotonic);

        assert(@intFromPtr(&self.index) != 0, "Storage index pointer cannot be null", .{});
        assert(@intFromPtr(&self.index.arena) != 0, "Storage index arena pointer cannot be null", .{});

        block.validate(self.backing_allocator) catch |err| {
            _ = self.storage_metrics.write_errors.fetchAdd(1, .monotonic);
            return err;
        };

        // Create WAL entry using backing allocator for temporary serialization
        const wal_entry = WALEntry.create_put_block(block, self.backing_allocator) catch |err| {
            _ = self.storage_metrics.write_errors.fetchAdd(1, .monotonic);
            return err;
        };
        defer wal_entry.deinit(self.backing_allocator);

        assert(wal_entry.entry_type == .put_block, "WAL entry type mismatch: expected put_block, got {}", .{wal_entry.entry_type});
        assert(wal_entry.entry_type == .put_block, "WAL entry type mismatch: expected put_block, got {}", .{wal_entry.entry_type});

        // Debug: Check payload before writing
        if (wal_entry.payload.len == 0) {
            std.debug.panic("WAL payload became empty after creation but before writing", .{});
        }

        // Write to WAL for durability
        self.write_wal_entry(wal_entry) catch |err| {
            _ = self.storage_metrics.write_errors.fetchAdd(1, .monotonic);
            _ = self.storage_metrics.wal_errors.fetchAdd(1, .monotonic);
            return err;
        };

        const index_size_before = self.index.block_count();

        // Catch corruption between WAL write and index insertion
        assert(block.version > 0, "Block version became invalid before index insertion", .{});
        assert(block.source_uri.len < 1024 * 1024, "Block source_uri became corrupted before index insertion", .{});

        self.index.put_block(block) catch |err| {
            _ = self.storage_metrics.write_errors.fetchAdd(1, .monotonic);
            return err;
        };

        const index_size_after = self.index.block_count();
        assert(index_size_after >= index_size_before, "Index size decreased after put: {} -> {}", .{ index_size_before, index_size_after });

        // Track successful operation
        _ = self.storage_metrics.blocks_written.fetchAdd(1, .monotonic);

        // Track bytes written (approximate block size)
        // Calculate approximate block size: strings + ID + version overhead
        const block_size = block.source_uri.len + block.metadata_json.len +
            block.content.len + 32;
        _ = self.storage_metrics.total_bytes_written.fetchAdd(block_size, .monotonic);

        const end_time = std.time.nanoTimestamp();
        const duration = @as(u64, @intCast(end_time - start_time));
        _ = self.storage_metrics.total_write_time_ns.fetchAdd(duration, .monotonic);

        // Check if we need to flush MemTable to SSTable based on memory usage
        if (self.index.memory_usage() >= self.config.memtable_max_size) {
            try self.flush_memtable_to_sstable();
        }
    }

    /// Find a Context Block by ID.
    pub fn find_block_by_id(
        self: *StorageEngine,
        block_id: BlockId,
    ) StorageError!*const ContextBlock {
        concurrency.assert_main_thread();

        assert(@intFromPtr(self) != 0, "StorageEngine self pointer cannot be null", .{});
        assert_state_valid(self.initialized, "StorageEngine must be initialized before find_block_by_id", .{});

        if (!self.initialized) return StorageError.NotInitialized;

        const start_time = std.time.nanoTimestamp();

        // Check memory first as it contains most recent writes
        if (self.index.find_block(block_id)) |block| {
            assert(@intFromPtr(block) != 0, "Index returned null block pointer", .{});
            assert(std.mem.eql(u8, &block.id.bytes, &block_id.bytes), "Found block ID mismatch: expected {} got {}", .{ block_id, block.id });

            _ = self.storage_metrics.blocks_read.fetchAdd(1, .monotonic);

            const block_size = block.source_uri.len + block.metadata_json.len +
                block.content.len + 32;
            _ = self.storage_metrics.total_bytes_read.fetchAdd(block_size, .monotonic);

            const end_time = std.time.nanoTimestamp();
            const duration = @as(u64, @intCast(end_time - start_time));
            _ = self.storage_metrics.total_read_time_ns.fetchAdd(duration, .monotonic);
            return block;
        }

        // Fall back to SSTables for older data not yet compacted
        for (self.sstables.items) |sstable_path| {
            assert_not_empty(sstable_path, "SSTable path cannot be empty", .{});

            const path_copy = self.backing_allocator.dupe(u8, sstable_path) catch |err| {
                _ = self.storage_metrics.read_errors.fetchAdd(1, .monotonic);
                return err;
            };
            var table = SSTable.init(self.backing_allocator, self.vfs, path_copy);
            defer table.deinit();

            _ = self.storage_metrics.sstable_reads.fetchAdd(1, .monotonic);
            table.read_index() catch continue;

            if (table.find_block(block_id) catch null) |block| {
                assert(std.mem.eql(u8, &block.id.bytes, &block_id.bytes), "SSTable block ID mismatch: expected {} got {}", .{ block_id, block.id });

                defer block.deinit(self.backing_allocator);

                // Cache in memory to avoid repeated SSTable reads
                self.index.put_block(block) catch {};

                _ = self.storage_metrics.blocks_read.fetchAdd(1, .monotonic);

                const block_size = block.source_uri.len + block.metadata_json.len +
                    block.content.len + 32;
                _ = self.storage_metrics.total_bytes_read.fetchAdd(block_size, .monotonic);

                const end_time = std.time.nanoTimestamp();
                const duration = @as(u64, @intCast(end_time - start_time));
                _ = self.storage_metrics.total_read_time_ns.fetchAdd(duration, .monotonic);

                const cached_block = self.index.find_block(block_id) orelse return error_context.storage_error(
                    StorageError.BlockNotFound,
                    error_context.block_context("find_block_after_sstable_transfer", block_id),
                );

                assert(std.mem.eql(u8, &cached_block.id.bytes, &block_id.bytes), "Cached block ID mismatch after SSTable transfer", .{});
                return cached_block;
            }
        }

        _ = self.storage_metrics.read_errors.fetchAdd(1, .monotonic);
        return StorageError.BlockNotFound;
    }

    /// Delete a Context Block by ID.
    pub fn delete_block(self: *StorageEngine, block_id: BlockId) !void {
        concurrency.assert_main_thread();
        assert(self.initialized, "StorageEngine must be initialized before delete_block", .{});
        if (!self.initialized) return StorageError.NotInitialized;

        // Create WAL entry
        const wal_entry = WALEntry.create_delete_block(
            block_id,
            self.backing_allocator,
        ) catch |err| {
            _ = self.storage_metrics.write_errors.fetchAdd(1, .monotonic);
            return err;
        };
        defer wal_entry.deinit(self.backing_allocator);

        // Write to WAL for durability
        self.write_wal_entry(wal_entry) catch |err| {
            _ = self.storage_metrics.write_errors.fetchAdd(1, .monotonic);
            _ = self.storage_metrics.wal_errors.fetchAdd(1, .monotonic);
            return err;
        };

        // Remove from in-memory index
        self.index.remove_block(block_id);

        // Remove associated edges from graph index
        self.graph_index.remove_block_edges(block_id);

        // Track successful deletion
        _ = self.storage_metrics.blocks_deleted.fetchAdd(1, .monotonic);
        _ = self.storage_metrics.wal_writes.fetchAdd(1, .monotonic);
    }

    /// Put a Graph Edge into storage.
    pub fn put_edge(self: *StorageEngine, edge: GraphEdge) !void {
        concurrency.assert_main_thread();
        assert(self.initialized, "StorageEngine must be initialized before put_edge", .{});
        if (!self.initialized) return StorageError.NotInitialized;

        // Create WAL entry
        const wal_entry = WALEntry.create_put_edge(edge, self.backing_allocator) catch |err| {
            _ = self.storage_metrics.write_errors.fetchAdd(1, .monotonic);
            return err;
        };
        defer wal_entry.deinit(self.backing_allocator);

        // Write to WAL for durability
        self.write_wal_entry(wal_entry) catch |err| {
            _ = self.storage_metrics.write_errors.fetchAdd(1, .monotonic);
            _ = self.storage_metrics.wal_errors.fetchAdd(1, .monotonic);
            return err;
        };

        // Add edge to graph index
        self.graph_index.put_edge(edge) catch |err| {
            _ = self.storage_metrics.write_errors.fetchAdd(1, .monotonic);
            return err;
        };

        // Track successful edge addition
        _ = self.storage_metrics.edges_added.fetchAdd(1, .monotonic);
        _ = self.storage_metrics.wal_writes.fetchAdd(1, .monotonic);
    }

    /// Get the current number of blocks in storage.
    pub fn block_count(self: *const StorageEngine) u32 {
        return self.index.block_count();
    }

    /// Get the current number of edges in storage.
    pub fn edge_count(self: *const StorageEngine) u32 {
        return self.graph_index.edge_count();
    }

    /// Get performance metrics for observability.
    pub fn metrics(self: *const StorageEngine) *const StorageMetrics {
        return &self.storage_metrics;
    }

    /// Find outgoing edges from a source block.
    pub fn find_outgoing_edges(self: *const StorageEngine, source_id: BlockId) ?[]const GraphEdge {
        return self.graph_index.find_outgoing_edges(source_id);
    }

    /// Find incoming edges to a target block.
    pub fn find_incoming_edges(self: *const StorageEngine, target_id: BlockId) ?[]const GraphEdge {
        return self.graph_index.find_incoming_edges(target_id);
    }

    /// Traverse outgoing edges using breadth-first search.
    pub fn traverse_outgoing(
        self: *const StorageEngine,
        start_id: BlockId,
        options: GraphEdgeIndex.TraversalOptions,
        allocator: std.mem.Allocator,
    ) !GraphEdgeIndex.TraversalResult {
        return self.graph_index.traverse_outgoing(start_id, options, allocator);
    }

    /// Traverse incoming edges using breadth-first search.
    pub fn traverse_incoming(
        self: *const StorageEngine,
        start_id: BlockId,
        options: GraphEdgeIndex.TraversalOptions,
        allocator: std.mem.Allocator,
    ) !GraphEdgeIndex.TraversalResult {
        return self.graph_index.traverse_incoming(start_id, options, allocator);
    }

    /// Traverse both incoming and outgoing edges using breadth-first search.
    pub fn traverse_bidirectional(
        self: *const StorageEngine,
        start_id: BlockId,
        options: GraphEdgeIndex.TraversalOptions,
        allocator: std.mem.Allocator,
    ) !GraphEdgeIndex.TraversalResult {
        return self.graph_index.traverse_bidirectional(start_id, options, allocator);
    }

    /// Iterator for all blocks in storage (memtable and SSTables).
    pub const BlockIterator = struct {
        engine: *const StorageEngine,
        memtable_iterator: ?std.HashMap(
            BlockId,
            ContextBlock,
            BlockIndex.BlockIdContext,
            std.hash_map.default_max_load_percentage,
        ).Iterator,

        /// Get the next block in the iteration.
        pub fn next(self: *BlockIterator) !?ContextBlock {
            // First iterate through memtable blocks
            if (self.memtable_iterator) |*iterator| {
                if (iterator.next()) |entry| {
                    return entry.value_ptr.*;
                } else {
                    // Memtable iteration complete, move to SSTables
                    self.memtable_iterator = null;
                    // SSTable iteration not yet implemented
                    // For now, just return null to end iteration
                    return null;
                }
            } else {
                // Already finished with memtable, and SSTable iteration not implemented yet
                return null;
            }
        }
    };

    /// Get an iterator for all blocks in storage.
    pub fn iterate_all_blocks(self: *const StorageEngine) BlockIterator {
        return BlockIterator{
            .engine = self,
            .memtable_iterator = self.index.blocks.iterator(),
        };
    }

    /// Flush WAL to disk.
    pub fn flush_wal(self: *StorageEngine) !void {
        const start_time = std.time.nanoTimestamp();

        if (self.wal_file) |*file| {
            file.flush() catch |err| {
                _ = self.storage_metrics.wal_errors.fetchAdd(1, .monotonic);
                return err;
            };
        }

        // Track successful WAL flush
        _ = self.storage_metrics.wal_flushes.fetchAdd(1, .monotonic);
        const end_time = std.time.nanoTimestamp();
        const duration = @as(u64, @intCast(end_time - start_time));
        _ = self.storage_metrics.total_wal_flush_time_ns.fetchAdd(duration, .monotonic);
    }

    /// Flushes the in-memory index (memtable) to a new, immutable SSTable on disk.
    ///
    /// ## Data Durability and Lifecycle
    ///
    /// This function is a critical part of the LSM-Tree lifecycle. The sequence of
    /// operations is designed to ensure durability without sacrificing performance:
    ///
    /// 1. All writes (`put_block`, `delete_block`) are first written to the Write-Ahead Log (WAL).
    ///    This guarantees that even if the server crashes, the operation is not lost.
    /// 2. The write is then applied to the in-memory `BlockIndex`.
    /// 3. When the `BlockIndex` reaches a certain size, this function is called.
    /// 4. It writes a complete, sorted, and immutable SSTable file to disk.
    /// 5. **Crucially**, only after the SSTable is successfully written and persisted is the
    ///    in-memory `BlockIndex` cleared via `arena.reset()`. This is safe because all the
    ///    data it contained is now durable in both the WAL and the new SSTable.
    pub fn flush_memtable_to_sstable(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
        assert(self.initialized, "StorageEngine must be initialized before flush_memtable_to_sstable", .{});
        if (!self.initialized) return StorageError.NotInitialized;

        if (self.index.block_count() == 0) {
            return; // Nothing to flush
        }

        // Create SSTable file path
        const sstable_path = try std.fmt.allocPrint(
            self.backing_allocator,
            "{s}/sst/sstable_{d:0>4}.sst",
            .{ self.data_dir, self.next_sstable_id },
        );

        // Collect all blocks from in-memory index
        var blocks_to_flush = std.ArrayList(ContextBlock).init(self.backing_allocator);
        defer blocks_to_flush.deinit();

        var iterator = self.index.blocks.iterator();
        while (iterator.next()) |entry| {
            try blocks_to_flush.append(entry.value_ptr.*);
        }

        if (blocks_to_flush.items.len == 0) {
            self.backing_allocator.free(sstable_path);
            return;
        }

        // Create and write SSTable
        var new_sstable = SSTable.init(self.backing_allocator, self.vfs, sstable_path);
        defer new_sstable.deinit();

        new_sstable.write_blocks(blocks_to_flush.items) catch |err| {
            _ = self.storage_metrics.write_errors.fetchAdd(1, .monotonic);
            return err;
        };

        // Track SSTable write
        _ = self.storage_metrics.sstable_writes.fetchAdd(1, .monotonic);

        // Add to SSTables list and compaction manager
        try self.sstables.append(try self.backing_allocator.dupe(u8, sstable_path));

        // Get file size for compaction manager
        const file_size = try self.read_file_size(sstable_path);
        try self.compaction_manager.add_sstable(sstable_path, file_size, 0); // L0 for new flushes

        self.next_sstable_id += 1;

        // Clear the in-memory index (blocks are now persisted in SSTable)
        self.index.clear();

        log.info(
            "Flushed {} blocks to SSTable: {s} (size: {} bytes)",
            .{ blocks_to_flush.items.len, sstable_path, file_size },
        );

        // Clean up old WAL segments now that data is persisted in SSTable
        // We can safely delete all segments before the current one since
        // all data has been flushed to the SSTable
        if (self.wal_segment_number > 0) {
            try self.cleanup_wal_segments(self.wal_segment_number);
        }

        // Check for compaction opportunities
        try self.check_and_run_compaction();
    }

    /// Check for compaction opportunities and execute if needed.
    fn check_and_run_compaction(self: *StorageEngine) !void {
        concurrency.assert_main_thread();

        if (self.compaction_manager.check_compaction_needed()) |job| {
            log.info(
                "Starting {s} compaction: L{} -> L{} ({} SSTables)",
                .{
                    @tagName(job.compaction_type),
                    job.input_level,
                    job.output_level,
                    job.input_paths.items.len,
                },
            );

            try self.compaction_manager.execute_compaction(job);

            // Update our SSTable tracking list
            try self.sync_sstable_list();
        }
    }

    /// Synchronize the storage engine's SSTable list with the compaction manager.
    fn sync_sstable_list(self: *StorageEngine) !void {
        _ = self; // SSTable synchronization not yet implemented
        // For now, we'll keep the simple list structure and let the compaction manager handle tiers
        // In a future optimization, we could remove this redundant tracking
    }

    /// Read the size of a file in bytes.
    fn read_file_size(self: *StorageEngine, path: []const u8) !u64 {
        var file = try self.vfs.open(path, .read);
        defer file.close() catch {};

        return try file.file_size();
    }

    /// Discover existing SSTable files and register them with the compaction manager.
    fn discover_existing_sstables(self: *StorageEngine) !void {
        const sst_dir = try std.fmt.allocPrint(self.backing_allocator, "{s}/sst", .{self.data_dir});
        defer self.backing_allocator.free(sst_dir);

        if (!self.vfs.exists(sst_dir)) {
            return; // No SSTable directory exists yet
        }

        // In a real implementation, we would iterate through files in the directory
        // For simulation VFS, we'll implement a simple discovery pattern
        // This is a simplified approach - production would parse file names for level info

        var sstable_id: u32 = 0;
        while (sstable_id < 1000) { // Check up to 1000 potential SSTables
            const sstable_path = try std.fmt.allocPrint(
                self.backing_allocator,
                "{s}/sstable_{d:0>4}.sst",
                .{ sst_dir, sstable_id },
            );
            defer self.backing_allocator.free(sstable_path);

            if (self.vfs.exists(sstable_path)) {
                const file_size = self.read_file_size(sstable_path) catch 0;

                // Add to our list
                const path_copy = try self.backing_allocator.dupe(u8, sstable_path);
                try self.sstables.append(path_copy);

                // Register with compaction manager (assume L0 for existing files)
                try self.compaction_manager.add_sstable(sstable_path, file_size, 0);

                // Update next ID
                if (sstable_id >= self.next_sstable_id) {
                    self.next_sstable_id = sstable_id + 1;
                }
            }

            sstable_id += 1;
        }
    }

    /// Write a WAL entry to the current WAL file.
    fn write_wal_entry(self: *StorageEngine, entry: WALEntry) !void {
        assert(@intFromPtr(self) != 0, "StorageEngine self pointer cannot be null", .{});
        if (self.wal_file == null) return StorageError.NotInitialized;
        // WAL entry payload should typically not be empty, but check the entry type first
        if (entry.entry_type == .put_block and entry.payload.len == 0) {
            std.debug.panic("WAL put_block entry payload cannot be empty - this indicates a serialization bug", .{});
        }
        // Validate that entry_type is a valid WAL entry type
        switch (entry.entry_type) {
            .put_block, .delete_block, .put_edge => {},
        }

        if (self.wal_file == null) return StorageError.NotInitialized;

        const serialized_size = WALEntry.HEADER_SIZE + entry.payload.len;

        assert(serialized_size > WALEntry.HEADER_SIZE, "Serialized size must be larger than header: {} <= {}", .{ serialized_size, WALEntry.HEADER_SIZE });
        assert(serialized_size <= MAX_WAL_SEGMENT_SIZE, "WAL entry too large: {} > {}", .{ serialized_size, MAX_WAL_SEGMENT_SIZE });

        // Check if rotation is needed before writing
        if (self.wal_segment_size + serialized_size > MAX_WAL_SEGMENT_SIZE) {
            try self.rotate_wal_segment();
        }

        const buffer = try self.backing_allocator.alloc(u8, serialized_size);
        defer self.backing_allocator.free(buffer);

        assert(buffer.len == serialized_size, "Buffer allocation size mismatch: {} != {}", .{ buffer.len, serialized_size });

        const written = try entry.serialize(buffer);

        assert(written == serialized_size, "Serialized size mismatch: {} != {}", .{ written, serialized_size });
        assert(written > 0, "Written bytes must be positive: {}", .{written});

        _ = try self.wal_file.?.write(buffer);

        // Update segment size and validate state
        const old_segment_size = self.wal_segment_size;
        self.wal_segment_size += written;

        assert(self.wal_segment_size > old_segment_size, "WAL segment size must increase: {} -> {}", .{ old_segment_size, self.wal_segment_size });
        assert(self.wal_segment_size <= MAX_WAL_SEGMENT_SIZE, "WAL segment size exceeded max: {} > {}", .{ self.wal_segment_size, MAX_WAL_SEGMENT_SIZE });

        _ = self.storage_metrics.wal_bytes_written.fetchAdd(buffer.len, .monotonic);
    }

    /// Rotate to a new WAL segment when current segment is full.
    fn rotate_wal_segment(self: *StorageEngine) !void {
        assert(self.wal_file != null, "WAL file must be open for rotation", .{});

        // Close current WAL file
        if (self.wal_file) |*file| {
            try file.flush();
            try file.close();
        }

        // Increment segment number
        self.wal_segment_number += 1;
        self.wal_segment_size = 0;

        // Create new WAL segment
        const wal_dir = try std.fmt.allocPrint(
            self.backing_allocator,
            "{s}/wal",
            .{self.data_dir},
        );
        defer self.backing_allocator.free(wal_dir);

        const new_wal_path = try std.fmt.allocPrint(
            self.backing_allocator,
            "{s}/wal_{d:0>4}.log",
            .{ wal_dir, self.wal_segment_number },
        );
        defer self.backing_allocator.free(new_wal_path);

        self.wal_file = try self.vfs.create(new_wal_path);

        log.info("Rotated WAL to segment {d}", .{self.wal_segment_number});
    }

    /// Clean up old WAL segments that have been flushed to SSTables.
    /// Keeps the current active segment and deletes all segments with
    /// numbers less than the specified threshold.
    fn cleanup_wal_segments(self: *StorageEngine, up_to_segment: u32) !void {
        // Only proceed if there are segments to clean
        if (up_to_segment == 0) {
            return;
        }

        const wal_dir = try std.fmt.allocPrint(
            self.backing_allocator,
            "{s}/wal",
            .{self.data_dir},
        );
        defer self.backing_allocator.free(wal_dir);

        // Delete all segments up to (but not including) the specified segment
        var segment_num: u32 = 0;
        while (segment_num < up_to_segment) : (segment_num += 1) {
            const segment_path = try std.fmt.allocPrint(
                self.backing_allocator,
                "{s}/wal_{d:0>4}.log",
                .{ wal_dir, segment_num },
            );
            defer self.backing_allocator.free(segment_path);

            // Only delete if the file exists
            if (self.vfs.exists(segment_path)) {
                self.vfs.remove(segment_path) catch |err| {
                    // Log error but continue cleanup
                    log.warn("Failed to delete WAL segment {s}: {any}", .{ segment_path, err });
                };
                log.info("Deleted WAL segment {d} after SSTable flush", .{segment_num});
            }
        }
    }

    /// Recover storage state from WAL files.
    pub fn recover_from_wal(self: *StorageEngine) !void {
        concurrency.assert_main_thread();
        assert(self.initialized, "StorageEngine must be initialized before WAL recovery", .{});
        if (!self.initialized) return StorageError.NotInitialized;

        // Track WAL recovery attempt
        _ = self.storage_metrics.wal_recoveries.fetchAdd(1, .monotonic);

        const wal_dir = try std.fmt.allocPrint(self.backing_allocator, "{s}/wal", .{self.data_dir});
        defer self.backing_allocator.free(wal_dir);

        // Check if WAL directory exists
        if (!self.vfs.exists(wal_dir)) {
            return; // No WAL files to recover
        }

        // Get list of WAL files
        const wal_files = try self.list_wal_files(wal_dir);
        defer {
            for (wal_files) |file_name| {
                self.backing_allocator.free(file_name);
            }
            self.backing_allocator.free(wal_files);
        }

        var entries_recovered: u32 = 0;
        var files_processed: u32 = 0;

        // Process each WAL file in chronological order
        for (wal_files) |file_name| {
            const file_path = try std.fmt.allocPrint(
                self.backing_allocator,
                "{s}/{s}",
                .{ wal_dir, file_name },
            );
            defer self.backing_allocator.free(file_path);

            const file_entries = self.recover_from_wal_file(file_path) catch |err| switch (err) {
                StorageError.InvalidChecksum, StorageError.InvalidWALEntryType => {
                    // Corruption detected - stop recovery at this point
                    log.warn("WAL corruption detected in {s}, stopping recovery", .{file_path});
                    break;
                },
                else => return err,
            };

            entries_recovered += file_entries;
            files_processed += 1;
        }

        log.info(
            "WAL recovery completed: {} files processed, {} entries recovered",
            .{ files_processed, entries_recovered },
        );
    }

    /// List WAL files in chronological order.
    fn list_wal_files(self: *StorageEngine, wal_dir: []const u8) ![][]const u8 {
        // Use VFS list_dir to get all files in the WAL directory
        const all_files = self.vfs.list_dir(wal_dir, self.backing_allocator) catch |err|
            switch (err) {
                error.FileNotFound => {
                    // Directory doesn't exist, return empty list
                    return try self.backing_allocator.alloc([]const u8, 0);
                },
                else => return err,
            };
        defer {
            for (all_files) |file_name| {
                self.backing_allocator.free(file_name);
            }
            self.backing_allocator.free(all_files);
        }

        var wal_files = std.ArrayList([]const u8).init(self.backing_allocator);
        defer wal_files.deinit();

        // Filter for WAL files (wal_XXXX.log pattern)
        for (all_files) |file_name| {
            if (std.mem.startsWith(u8, file_name, "wal_") and
                std.mem.endsWith(u8, file_name, ".log"))
            {
                try wal_files.append(try self.backing_allocator.dupe(u8, file_name));
            }
        }

        // Sort WAL files in chronological order (by filename)
        const wal_file_slice = try wal_files.toOwnedSlice();
        std.mem.sort([]const u8, wal_file_slice, {}, struct {
            fn less_than(context: void, a: []const u8, b: []const u8) bool {
                _ = context;
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less_than);

        return wal_file_slice;
    }

    /// Recover from a single WAL file using streaming approach.
    /// Reads and processes one WAL entry at a time to avoid loading entire file into memory.
    fn recover_from_wal_file(self: *StorageEngine, file_path: []const u8) !u32 {
        var file = self.vfs.open(file_path, .read) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        defer file.close() catch {};

        const file_size = try file.file_size();
        if (file_size == 0) return 0;

        var entries_recovered: u32 = 0;
        var file_offset: u64 = 0;

        // Arena prevents memory leaks during entry deserialization
        // Reset periodically to prevent unbounded growth during large recoveries
        var arena = std.heap.ArenaAllocator.init(self.backing_allocator);
        defer arena.deinit();
        const temp_allocator = arena.allocator();

        // Buffer for reading WAL entry headers - small, fixed size
        var header_buffer: [WALEntry.HEADER_SIZE]u8 = undefined;

        while (file_offset < file_size) {
            // Check if we have enough bytes left for a complete header
            if (file_offset + WALEntry.HEADER_SIZE > file_size) {
                // Incomplete header at file end - stop recovery
                break;
            }

            // Seek to current position and read entry header
            _ = try file.seek(@intCast(file_offset), .start);
            const header_bytes_read = try file.read(&header_buffer);
            if (header_bytes_read < WALEntry.HEADER_SIZE) {
                // EOF reached before complete header - stop recovery
                break;
            }

            // Parse header to get payload size (offset 9 = 8 bytes checksum + 1 byte type)
            const payload_size = std.mem.readInt(u32, header_buffer[9..][0..4], .little);
            const total_entry_size = WALEntry.HEADER_SIZE + payload_size;

            // Validate we have enough bytes for complete entry
            if (file_offset + total_entry_size > file_size) {
                // Incomplete entry at file end - stop recovery
                break;
            }

            // Allocate buffer for complete entry (header + payload)
            const entry_buffer = temp_allocator.alloc(u8, total_entry_size) catch |err| {
                return error_context.storage_error(
                    err,
                    error_context.file_context("allocate_wal_entry_buffer", file_path),
                );
            };

            // Copy header and read payload
            @memcpy(entry_buffer[0..WALEntry.HEADER_SIZE], &header_buffer);
            _ = try file.seek(@intCast(file_offset + WALEntry.HEADER_SIZE), .start);
            const payload_bytes_read = try file.read(entry_buffer[WALEntry.HEADER_SIZE..]);
            if (payload_bytes_read < payload_size) {
                // EOF reached before complete payload - stop recovery
                break;
            }

            // Deserialize and apply entry
            const entry = WALEntry.deserialize(entry_buffer, temp_allocator) catch |err|
                switch (err) {
                    StorageError.InvalidChecksum => {
                        // Corruption detected - stop recovery at this point
                        log.warn("WAL entry checksum mismatch at offset {d} in {s}, stopping recovery", .{ file_offset, file_path });
                        break;
                    },
                    else => return err,
                };

            try self.apply_wal_entry(entry);

            file_offset += total_entry_size;
            entries_recovered += 1;

            // Periodic arena reset to prevent memory growth during large recovery
            // Reset every 1000 entries to balance memory usage vs allocation overhead
            if (entries_recovered % 1000 == 0) {
                _ = arena.reset(.retain_capacity);
            }
        }

        return entries_recovered;
    }

    /// Apply a single WAL entry to rebuild storage state.
    fn apply_wal_entry(self: *StorageEngine, entry: WALEntry) !void {
        switch (entry.entry_type) {
            .put_block => {
                // Use main allocator for deserializing to avoid memory corruption
                const block = try ContextBlock.deserialize(entry.payload, self.backing_allocator);

                // Add block to in-memory index (put_block will clone the strings)
                try self.index.put_block(block);

                // Free the temporary block after put_block completes
                block.deinit(self.backing_allocator);
            },
            .delete_block => {
                if (entry.payload.len != 16) return error.InvalidPayloadSize;

                const block_id = BlockId{ .bytes = entry.payload[0..16].* };
                self.index.remove_block(block_id);
                self.graph_index.remove_block_edges(block_id);
            },
            .put_edge => {
                if (entry.payload.len != 40) return error.InvalidPayloadSize;

                const edge = try GraphEdge.deserialize(entry.payload);
                try self.graph_index.put_edge(edge);
            },
        }
    }
};

// Tests

test "WALEntry serialization roundtrip" {
    const allocator = std.testing.allocator;

    // Create test block
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"test\":true}",
        .content = "test content",
    };

    // Create WAL entry
    const wal_entry = try WALEntry.create_put_block(test_block, allocator);
    defer wal_entry.deinit(allocator);

    // Serialize
    const serialized_size = WALEntry.HEADER_SIZE + wal_entry.payload.len;
    const buffer = try allocator.alloc(u8, serialized_size);
    defer allocator.free(buffer);

    const written = try wal_entry.serialize(buffer);
    try std.testing.expectEqual(serialized_size, written);

    // Deserialize
    const deserialized = try WALEntry.deserialize(buffer, allocator);
    defer deserialized.deinit(allocator);

    // Verify
    try std.testing.expectEqual(wal_entry.checksum, deserialized.checksum);
    try std.testing.expectEqual(wal_entry.entry_type, deserialized.entry_type);
    try std.testing.expectEqualSlices(u8, wal_entry.payload, deserialized.payload);
}

test "BlockIndex basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const backing_allocator = gpa.allocator();

    // Use arena allocator to match production usage
    var arena = std.heap.ArenaAllocator.init(backing_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var index = try BlockIndex.init(allocator);
    defer index.deinit();

    // Test empty index
    try std.testing.expectEqual(@as(u32, 0), index.block_count());

    // Create test block
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"test\":true}",
        .content = "test content",
    };

    // Test put and get
    try index.put_block(test_block);
    try std.testing.expectEqual(@as(u32, 1), index.block_count());

    const retrieved = index.find_block(test_id);
    try std.testing.expect(retrieved != null);
    try std.testing.expect(retrieved.?.id.eql(test_id));
    try std.testing.expectEqual(@as(u64, 1), retrieved.?.version);

    // Test remove
    index.remove_block(test_id);
    try std.testing.expectEqual(@as(u32, 0), index.block_count());
    try std.testing.expect(index.find_block(test_id) == null);
}

test "StorageEngine basic operations" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    // Create simulation VFS
    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_data");
    defer allocator.free(data_dir);

    var storage = try StorageEngine.init_default(allocator, vfs_interface, data_dir);
    defer storage.deinit();

    // Initialize storage
    try storage.initialize_storage();

    // Create test block
    const test_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://uri",
        .metadata_json = "{\"test\":true}",
        .content = "test content",
    };

    // Test put and get
    try storage.put_block(test_block);
    try std.testing.expectEqual(@as(u32, 1), storage.block_count());

    const retrieved = try storage.find_block_by_id(test_id);
    try std.testing.expect(retrieved.id.eql(test_id));
    try std.testing.expectEqual(@as(u64, 1), retrieved.version);

    // Test delete
    try storage.delete_block(test_id);
    try std.testing.expectEqual(@as(u32, 0), storage.block_count());
    try std.testing.expectError(StorageError.BlockNotFound, storage.find_block_by_id(test_id));
}

test "StorageEngine graph edge operations" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "test_data");
    defer allocator.free(data_dir);

    var storage = try StorageEngine.init_default(allocator, vfs_interface, data_dir);
    defer storage.deinit();

    try storage.initialize_storage();

    // Create test edge
    const source_id = try BlockId.from_hex("0123456789abcdeffedcba9876543210");
    const target_id = try BlockId.from_hex("fedcba9876543210123456789abcdef0");
    const test_edge = GraphEdge{
        .source_id = source_id,
        .target_id = target_id,
        .edge_type = .imports,
    };

    // Test put edge (should not error)
    try storage.put_edge(test_edge);
}

test "StorageEngine graph edge indexing" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();

    const data_dir = try allocator.dupe(u8, "/test");
    defer allocator.free(data_dir);
    var storage = try StorageEngine.init_default(allocator, vfs_interface, data_dir);
    defer storage.deinit();

    try storage.startup();

    // Create test blocks
    const block1_id = try BlockId.from_hex("11111111111111111111111111111111");
    const block2_id = try BlockId.from_hex("22222222222222222222222222222222");
    const block3_id = try BlockId.from_hex("33333333333333333333333333333333");

    const block1 = ContextBlock{
        .id = block1_id,
        .version = 1,
        .source_uri = "test://block1.zig",
        .metadata_json = "{\"type\":\"function\"}",
        .content = "pub fn block1() void {}",
    };

    const block2 = ContextBlock{
        .id = block2_id,
        .version = 1,
        .source_uri = "test://block2.zig",
        .metadata_json = "{\"type\":\"struct\"}",
        .content = "const Block2 = struct {};",
    };

    const block3 = ContextBlock{
        .id = block3_id,
        .version = 1,
        .source_uri = "test://block3.zig",
        .metadata_json = "{\"type\":\"constant\"}",
        .content = "const VALUE = 42;",
    };

    // Put blocks
    try storage.put_block(block1);
    try storage.put_block(block2);
    try storage.put_block(block3);

    // Create test edges
    const edge1 = GraphEdge{
        .source_id = block1_id,
        .target_id = block2_id,
        .edge_type = .imports,
    };

    const edge2 = GraphEdge{
        .source_id = block1_id,
        .target_id = block3_id,
        .edge_type = .references,
    };

    const edge3 = GraphEdge{
        .source_id = block2_id,
        .target_id = block3_id,
        .edge_type = .contains,
    };

    // Put edges
    try storage.put_edge(edge1);
    try storage.put_edge(edge2);
    try storage.put_edge(edge3);

    // Verify edge count
    try std.testing.expectEqual(@as(u32, 3), storage.edge_count());

    // Test outgoing edges
    const outgoing_from_block1 = storage.find_outgoing_edges(block1_id);
    try std.testing.expect(outgoing_from_block1 != null);
    try std.testing.expectEqual(@as(usize, 2), outgoing_from_block1.?.len);

    const outgoing_from_block2 = storage.find_outgoing_edges(block2_id);
    try std.testing.expect(outgoing_from_block2 != null);
    try std.testing.expectEqual(@as(usize, 1), outgoing_from_block2.?.len);

    const outgoing_from_block3 = storage.find_outgoing_edges(block3_id);
    try std.testing.expect(outgoing_from_block3 == null);

    // Test incoming edges
    const incoming_to_block1 = storage.find_incoming_edges(block1_id);
    try std.testing.expect(incoming_to_block1 == null);

    const incoming_to_block2 = storage.find_incoming_edges(block2_id);
    try std.testing.expect(incoming_to_block2 != null);
    try std.testing.expectEqual(@as(usize, 1), incoming_to_block2.?.len);

    const incoming_to_block3 = storage.find_incoming_edges(block3_id);
    try std.testing.expect(incoming_to_block3 != null);
    try std.testing.expectEqual(@as(usize, 2), incoming_to_block3.?.len);

    // Test edge types
    for (outgoing_from_block1.?) |edge| {
        if (edge.target_id.eql(block2_id)) {
            try std.testing.expectEqual(EdgeType.imports, edge.edge_type);
        } else if (edge.target_id.eql(block3_id)) {
            try std.testing.expectEqual(EdgeType.references, edge.edge_type);
        }
    }

    // Test block deletion removes edges
    try storage.delete_block(block1_id);

    // Verify edges involving block1 are removed
    try std.testing.expectEqual(@as(u32, 1), storage.edge_count());
    try std.testing.expect(storage.find_outgoing_edges(block1_id) == null);
    try std.testing.expect(storage.find_incoming_edges(block1_id) == null);

    const remaining_incoming_to_block2 = storage.find_incoming_edges(block2_id);
    try std.testing.expect(remaining_incoming_to_block2 == null);

    const remaining_incoming_to_block3 = storage.find_incoming_edges(block3_id);
    try std.testing.expect(remaining_incoming_to_block3 != null);
    try std.testing.expectEqual(@as(usize, 1), remaining_incoming_to_block3.?.len);
}

test "StorageEngine graph edge WAL recovery" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = "/test_wal_edges";

    // First storage instance - write edges
    {
        const owned_data_dir = try allocator.dupe(u8, data_dir);
        defer allocator.free(owned_data_dir);
        var storage = try StorageEngine.init_default(
            allocator,
            vfs_interface,
            owned_data_dir,
        );
        defer storage.deinit();

        try storage.startup();

        // Create test blocks and edges
        const block1_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        const block2_id = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        const block1 = ContextBlock{
            .id = block1_id,
            .version = 1,
            .source_uri = "test://module1.zig",
            .metadata_json = "{\"type\":\"module\"}",
            .content = "const module1 = @import(\"module2.zig\");",
        };

        const block2 = ContextBlock{
            .id = block2_id,
            .version = 1,
            .source_uri = "test://module2.zig",
            .metadata_json = "{\"type\":\"module\"}",
            .content = "pub fn exported_function() void {}",
        };

        try storage.put_block(block1);
        try storage.put_block(block2);

        const edge = GraphEdge{
            .source_id = block1_id,
            .target_id = block2_id,
            .edge_type = .imports,
        };

        try storage.put_edge(edge);

        // Verify edge was stored
        try std.testing.expectEqual(@as(u32, 1), storage.edge_count());

        try storage.flush_wal();
    }

    // Second storage instance - recover from WAL
    {
        const owned_data_dir = try allocator.dupe(u8, data_dir);
        defer allocator.free(owned_data_dir);
        var storage = try StorageEngine.init_default(
            allocator,
            vfs_interface,
            owned_data_dir,
        );
        defer storage.deinit();

        try storage.startup();

        // Verify edge was recovered
        try std.testing.expectEqual(@as(u32, 1), storage.edge_count());

        const block1_id = try BlockId.from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        const block2_id = try BlockId.from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        const outgoing_edges = storage.find_outgoing_edges(block1_id);
        try std.testing.expect(outgoing_edges != null);
        try std.testing.expectEqual(@as(usize, 1), outgoing_edges.?.len);
        try std.testing.expect(outgoing_edges.?[0].target_id.eql(block2_id));
        try std.testing.expectEqual(EdgeType.imports, outgoing_edges.?[0].edge_type);

        const incoming_edges = storage.find_incoming_edges(block2_id);
        try std.testing.expect(incoming_edges != null);
        try std.testing.expectEqual(@as(usize, 1), incoming_edges.?.len);
        try std.testing.expect(incoming_edges.?[0].source_id.eql(block1_id));
    }
}

test "StorageMetrics formatting methods" {
    var metrics = StorageMetrics.init();

    // Simulate some activity
    _ = metrics.blocks_written.fetchAdd(5, .monotonic);
    _ = metrics.blocks_read.fetchAdd(3, .monotonic);
    _ = metrics.total_bytes_written.fetchAdd(1024, .monotonic);
    _ = metrics.total_bytes_read.fetchAdd(512, .monotonic);

    // Test human-readable formatting
    var buffer: [2048]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    try metrics.format_human_readable(stream.writer());

    const output = stream.getWritten();
    try std.testing.expect(std.mem.indexOf(u8, output, "Storage Metrics") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "5 written") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "3 read") != null);

    // Test JSON formatting
    stream.reset();
    try metrics.format_json(stream.writer());

    const json_output = stream.getWritten();
    try std.testing.expect(std.mem.indexOf(u8, json_output, "\"blocks_written\": 5") != null);
    try std.testing.expect(std.mem.indexOf(u8, json_output, "\"blocks_read\": 3") != null);
}

test "StorageEngine metrics and observability" {
    const allocator = std.testing.allocator;
    const simulation_vfs = @import("simulation_vfs");

    var sim_vfs = simulation_vfs.SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    const vfs_interface = sim_vfs.vfs();
    const data_dir = try allocator.dupe(u8, "metrics_test_data");
    defer allocator.free(data_dir);

    var storage = try StorageEngine.init_default(allocator, vfs_interface, data_dir);
    defer storage.deinit();

    try storage.initialize_storage();

    // Get initial metrics
    const initial_metrics = storage.metrics();
    try std.testing.expectEqual(@as(u64, 0), initial_metrics.blocks_written.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), initial_metrics.blocks_read.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), initial_metrics.wal_writes.load(.monotonic));

    // Create test block
    const test_id = try BlockId.from_hex("deadbeefdeadbeefdeadbeefdeadbeef");
    const test_block = ContextBlock{
        .id = test_id,
        .version = 1,
        .source_uri = "test://metrics.zig",
        .metadata_json = "{\"type\":\"metrics_test\"}",
        .content = "pub fn metrics_test() void {}",
    };

    // Test block write metrics
    try storage.put_block(test_block);

    const after_write_metrics = storage.metrics();
    try std.testing.expectEqual(@as(u64, 1), after_write_metrics.blocks_written.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), after_write_metrics.wal_writes.load(.monotonic));
    try std.testing.expect(after_write_metrics.total_write_time_ns.load(.monotonic) > 0);

    // Test block read metrics
    const found_block = try storage.find_block_by_id(test_id);
    try std.testing.expect(found_block.id.eql(test_id));

    const after_read_metrics = storage.metrics();
    try std.testing.expectEqual(@as(u64, 1), after_read_metrics.blocks_read.load(.monotonic));
    try std.testing.expect(after_read_metrics.total_read_time_ns.load(.monotonic) > 0);

    // Test WAL flush metrics
    try storage.flush_wal();

    const after_flush_metrics = storage.metrics();
    try std.testing.expectEqual(@as(u64, 1), after_flush_metrics.wal_flushes.load(.monotonic));
    try std.testing.expect(after_flush_metrics.total_wal_flush_time_ns.load(.monotonic) > 0);

    // Test edge metrics
    const edge = GraphEdge{
        .source_id = test_id,
        .target_id = try BlockId.from_hex("cafebabecafebabecafebabecafebabe"),
        .edge_type = .calls,
    };

    try storage.put_edge(edge);

    const after_edge_metrics = storage.metrics();
    try std.testing.expectEqual(@as(u64, 1), after_edge_metrics.edges_added.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 2), after_edge_metrics.wal_writes.load(.monotonic));
    // +1 for edge

    // Test block deletion metrics
    try storage.delete_block(test_id);

    const after_delete_metrics = storage.metrics();
    try std.testing.expectEqual(@as(u64, 1), after_delete_metrics.blocks_deleted.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 3), after_delete_metrics.wal_writes.load(.monotonic));
    // +1 for delete

    // Test average latency calculations
    try std.testing.expect(after_read_metrics.average_read_latency_ns() > 0);
    try std.testing.expect(after_write_metrics.average_write_latency_ns() > 0);
    try std.testing.expect(after_flush_metrics.average_wal_flush_latency_ns() > 0);

    // Test error-free operations
    try std.testing.expectEqual(@as(u64, 0), after_delete_metrics.write_errors.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), after_delete_metrics.read_errors.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), after_delete_metrics.wal_errors.load(.monotonic));
}
