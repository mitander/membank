//! Comprehensive memory model integration for KausalDB.
//!
//! This module demonstrates the complete memory architecture with all Week 1-4 improvements
//! integrated together: TypedStorageCoordinator, ObjectPools, Data-Oriented Design,
//! and Zero-Copy patterns working as a cohesive system.
//!
//! **Architecture Overview:**
//! - Level 1: Fixed-size object pools for high-frequency objects
//! - Level 2: Arena coordinators for subsystem memory management
//! - Level 3: Data-oriented storage layouts for cache optimization
//! - Level 4: Zero-copy query paths for allocation-free reads
//! - Level 5: Type-safe coordinators eliminating *anyopaque patterns

const std = @import("std");
const builtin = @import("builtin");
const assert = @import("assert.zig").assert;
const fatal_assert = @import("assert.zig").fatal_assert;
const memory = @import("memory.zig");
const pools = @import("pools.zig");
const ownership = @import("ownership.zig");
const PerformanceAssertion = @import("../testing/performance_assertions.zig");
const context_block = @import("types.zig");

const ArenaCoordinator = memory.ArenaCoordinator;
const TypedStorageCoordinatorType = memory.TypedStorageCoordinatorType;
const ObjectPoolType = pools.ObjectPoolType;
const PoolManagerType = pools.PoolManagerType;
const StackPoolType = pools.StackPoolType;
const OwnedBlock = ownership.OwnedBlock;
const BlockOwnership = ownership.BlockOwnership;
const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;

/// Integrated memory management system combining all architectural improvements.
/// Demonstrates the complete memory model working together as designed.
pub const IntegratedMemorySystem = struct {
    /// Backing allocator for infrastructure (Level 1: Permanent Infrastructure)
    backing_allocator: std.mem.Allocator,

    // Level 2: Arena-per-subsystem with coordinator interface
    subsystem_arena: *std.heap.ArenaAllocator,
    arena_coordinator: ArenaCoordinator,

    /// Object pools for high-frequency allocations (Level 1: Object Pools)
    sstable_pool: ObjectPoolType(MockSSTable),
    iterator_pool: ObjectPoolType(MockIterator),

    /// Stack pool for ultra-fast temporary objects (Level 1: Stack Pools)
    temp_buffer_pool: StackPoolType(TempBuffer, 16),

    /// Typed storage coordinator (Type Safety)
    storage_coordinator: TypedStorageCoordinatorType(IntegratedMemorySystem),

    /// Data-oriented storage demo (Level 3: Cache Optimization)
    block_metadata: BlockMetadataSOA,

    /// Zero-copy query session (Level 4: Zero-Copy)
    query_session: QuerySession,

    /// Performance metrics
    allocation_metrics: AllocationMetrics,

    const Self = @This();

    /// Mock SSTable for demonstration
    const MockSSTable = struct {
        file_path: []const u8,
        block_count: u32,
        is_active: bool,

        pub fn init(path: []const u8) MockSSTable {
            return MockSSTable{
                .file_path = path,
                .block_count = 0,
                .is_active = true,
            };
        }

        pub fn deinit(self: *MockSSTable) void {
            self.is_active = false;
        }
    };

    /// Mock Iterator for demonstration
    const MockIterator = struct {
        position: u32,
        total_items: u32,
        is_active: bool,

        pub fn init(total: u32) MockIterator {
            return MockIterator{
                .position = 0,
                .total_items = total,
                .is_active = true,
            };
        }

        pub fn deinit(self: *MockIterator) void {
            self.is_active = false;
        }
    };

    /// Temporary buffer for stack pool demonstration
    const TempBuffer = struct {
        data: [1024]u8,
        size: u32,

        pub fn init() TempBuffer {
            return TempBuffer{
                .data = undefined,
                .size = 0,
            };
        }
    };

    /// Data-oriented block metadata storage (Struct-of-Arrays)
    const BlockMetadataSOA = struct {
        ids: std.ArrayList(BlockId),
        versions: std.ArrayList(u64),
        sizes: std.ArrayList(u32),
        timestamps: std.ArrayList(i64),

        pub fn init(allocator: std.mem.Allocator) BlockMetadataSOA {
            return BlockMetadataSOA{
                .ids = std.ArrayList(BlockId).init(allocator),
                .versions = std.ArrayList(u64).init(allocator),
                .sizes = std.ArrayList(u32).init(allocator),
                .timestamps = std.ArrayList(i64).init(allocator),
            };
        }

        pub fn deinit(self: *BlockMetadataSOA) void {
            self.ids.deinit();
            self.versions.deinit();
            self.sizes.deinit();
            self.timestamps.deinit();
        }

        /// Cache-optimal scan for blocks newer than timestamp
        pub fn find_recent_blocks(
            self: *const BlockMetadataSOA,
            min_timestamp: i64,
            allocator: std.mem.Allocator,
        ) ![]BlockId {
            var results = std.ArrayList(BlockId).init(allocator);
            try results.ensureTotalCapacity(self.ids.items.len / 4); // Estimate 25% match rate
            errdefer results.deinit();

            // Linear scan through timestamps array - very cache friendly
            for (self.timestamps.items, 0..) |timestamp, i| {
                if (timestamp >= min_timestamp) {
                    try results.append(self.ids.items[i]);
                }
            }

            return results.toOwnedSlice();
        }
    };

    /// Zero-copy query session for allocation-free reads
    const QuerySession = struct {
        session_id: u64,
        is_active: bool,

        pub fn init() QuerySession {
            return QuerySession{
                .session_id = @as(u64, @intCast(std.time.nanoTimestamp())),
                .is_active = true,
            };
        }

        pub fn invalidate(self: *QuerySession) void {
            self.is_active = false;
        }

        pub fn validate(self: *const QuerySession) void {
            if (comptime builtin.mode == .Debug) {
                fatal_assert(self.is_active, "Query session invalidated", .{});
            }
        }
    };

    /// Performance metrics tracking
    const AllocationMetrics = struct {
        pool_acquisitions: u64,
        arena_allocations: u64,
        zero_copy_accesses: u64,
        cache_hits: u64,
        cache_misses: u64,

        pub fn init() AllocationMetrics {
            return AllocationMetrics{
                .pool_acquisitions = 0,
                .arena_allocations = 0,
                .zero_copy_accesses = 0,
                .cache_hits = 0,
                .cache_misses = 0,
            };
        }

        /// Report allocation metrics to standard logging.
        /// Provides overview of memory system performance and cache efficiency.
        pub fn report(self: *const AllocationMetrics) void {
            std.log.info("=== Memory System Performance Metrics ===", .{});
            std.log.info("Pool acquisitions: {}", .{self.pool_acquisitions});
            std.log.info("Arena allocations: {}", .{self.arena_allocations});
            std.log.info("Zero-copy accesses: {}", .{self.zero_copy_accesses});
            std.log.info("Cache hit ratio: {d:.2}%", .{if (self.cache_hits + self.cache_misses > 0)
                @as(f64, @floatFromInt(self.cache_hits)) / @as(f64, @floatFromInt(self.cache_hits + self.cache_misses)) * 100.0
            else
                0.0});
        }
    };

    /// Initialize integrated memory system with all architectural improvements.
    pub fn init(backing_allocator: std.mem.Allocator) !Self {
        // Allocate arena on heap to ensure stable address across struct moves
        const subsystem_arena = try backing_allocator.create(std.heap.ArenaAllocator);
        subsystem_arena.* = std.heap.ArenaAllocator.init(backing_allocator);

        var self = Self{
            .backing_allocator = backing_allocator,
            .subsystem_arena = subsystem_arena,
            .arena_coordinator = ArenaCoordinator.init(subsystem_arena),
            .sstable_pool = try ObjectPoolType(MockSSTable).init(backing_allocator, 32),
            .iterator_pool = try ObjectPoolType(MockIterator).init(backing_allocator, 64),
            .temp_buffer_pool = StackPoolType(TempBuffer, 16).init(),
            .storage_coordinator = undefined, // Will be set after construction
            .block_metadata = BlockMetadataSOA.init(backing_allocator),
            .query_session = QuerySession.init(),
            .allocation_metrics = AllocationMetrics.init(),
        };

        // Initialize typed storage coordinator pointing to self
        // Note: This creates a self-reference for demonstration purposes
        self.storage_coordinator = TypedStorageCoordinatorType(IntegratedMemorySystem).init(@ptrCast(&self));

        return self;
    }

    /// Demonstrate the complete memory lifecycle with all patterns.
    pub fn demonstrate_memory_lifecycle(self: *Self) !void {
        std.log.info("=== Demonstrating Integrated Memory System ===", .{});

        // Object pools provide O(1) allocation for high-frequency objects
        std.log.info("1. Object Pool Demonstration", .{});
        const sstable = self.sstable_pool.acquire() orelse return error.PoolExhausted;
        sstable.* = MockSSTable.init("demo.sst");
        self.allocation_metrics.pool_acquisitions += 1;
        std.log.info("   - Acquired SSTable from pool: {s}", .{sstable.file_path});

        // Arena allocation enables bulk deallocation for subsystem memory
        std.log.info("2. Arena Allocation Demonstration", .{});
        // Simulate arena allocation without actually allocating persistent memory
        self.allocation_metrics.arena_allocations += 1;
        std.log.info("   - Simulated 1024 bytes arena allocation", .{});

        // Stack pools provide ultra-fast allocation for temporary objects
        std.log.info("3. Stack Pool Demonstration", .{});
        const temp_buffer = self.temp_buffer_pool.acquire() orelse return error.StackExhausted;
        temp_buffer.size = 512;
        std.log.info("   - Acquired temporary buffer of {} bytes", .{temp_buffer.size});

        // SOA layout optimizes cache performance for bulk operations
        std.log.info("4. Data-Oriented Storage Demonstration", .{});
        // Safety: Hard-coded hex string is guaranteed to be valid BlockId format
        const demo_id = BlockId.from_hex("1234567890abcdef1234567890abcdef") catch unreachable;
        try self.block_metadata.ids.append(demo_id);
        try self.block_metadata.versions.append(42);
        try self.block_metadata.sizes.append(1024);
        try self.block_metadata.timestamps.append(@as(i64, @intCast(std.time.nanoTimestamp())));
        std.log.info("   - Added block metadata in SOA layout", .{});

        // Zero-copy eliminates allocation overhead on read paths
        std.log.info("5. Zero-Copy Access Demonstration", .{});
        self.query_session.validate();
        self.allocation_metrics.zero_copy_accesses += 1;
        std.log.info("   - Validated zero-copy query session", .{});

        // Type-safe coordinators eliminate *anyopaque patterns for compile-time validation
        std.log.info("6. Type-Safe Coordinator Demonstration", .{});
        self.storage_coordinator.validate_coordinator();
        std.log.info("   - Validated typed storage coordinator", .{});

        // SOA scans demonstrate cache performance benefits over AOS layouts
        std.log.info("7. Cache-Optimal Scan Demonstration", .{});
        const recent_blocks = try self.block_metadata.find_recent_blocks(0, self.backing_allocator);
        defer self.backing_allocator.free(recent_blocks);
        self.allocation_metrics.cache_hits += recent_blocks.len;
        std.log.info("   - Found {} recent blocks via SOA scan", .{recent_blocks.len});

        // 8. Performance Metrics
        self.allocation_metrics.report();

        // Cleanup demonstration
        self.temp_buffer_pool.release(temp_buffer);
        self.sstable_pool.release(sstable);

        // Clean up block metadata added during demo to prevent leaks
        self.block_metadata.ids.clearRetainingCapacity();
        self.block_metadata.versions.clearRetainingCapacity();
        self.block_metadata.sizes.clearRetainingCapacity();
        self.block_metadata.timestamps.clearRetainingCapacity();

        // Reset arena to clean up demonstration allocations
        self.arena_coordinator.reset();

        std.log.info("=== Demonstration Complete ===", .{});
    }

    /// Benchmark the performance benefits of the integrated system.
    pub fn benchmark_performance(self: *Self, iterations: u32) !PerformanceBenchmark {
        // Use task arena for benchmark allocations to avoid GPA leak detection
        var task_arena = std.heap.ArenaAllocator.init(self.backing_allocator);
        defer task_arena.deinit();
        const task_coordinator = ArenaCoordinator.init(&task_arena);

        const start_time = std.time.nanoTimestamp();

        var i: u32 = 0;
        while (i < iterations) : (i += 1) {
            // Simulate typical operations with all memory patterns

            // Pool allocation/deallocation
            if (self.sstable_pool.acquire()) |sstable| {
                sstable.* = MockSSTable.init("bench.sst");
                self.sstable_pool.release(sstable);
                self.allocation_metrics.pool_acquisitions += 1;
            }

            // Arena allocation using task arena (properly cleaned up)
            const small_alloc = try task_coordinator.alloc(u8, 64);
            self.allocation_metrics.arena_allocations += 1;
            _ = small_alloc;

            // Zero-copy access
            self.query_session.validate();
            self.allocation_metrics.zero_copy_accesses += 1;
        }

        const end_time = std.time.nanoTimestamp();
        const total_ns = end_time - start_time;

        return PerformanceBenchmark{
            .total_time_ns = @as(u64, @intCast(total_ns)),
            .operations_per_second = @as(f64, @floatFromInt(iterations)) / (@as(f64, @floatFromInt(total_ns)) / 1_000_000_000.0),
            .ns_per_operation = @as(u64, @intCast(@divTrunc(total_ns, iterations))),
            .memory_efficiency = self.calculate_memory_efficiency(),
        };
    }

    /// Calculate memory efficiency metrics.
    fn calculate_memory_efficiency(self: *const Self) f64 {
        const pool_utilization = self.sstable_pool.utilization() + self.iterator_pool.utilization();
        const temp_utilization = @as(f64, @floatFromInt(self.temp_buffer_pool.active_count())) / 16.0;
        return (pool_utilization + temp_utilization) / 3.0; // Average utilization
    }

    /// Reset all subsystem memory while preserving structure.
    pub fn reset_subsystems(self: *Self) void {
        std.log.info("Resetting all subsystem memory...", .{});

        // Reset arena through coordinator (O(1) bulk cleanup)
        self.arena_coordinator.reset();

        // Reset object pools
        self.sstable_pool.reset();
        self.iterator_pool.reset();
        self.temp_buffer_pool.reset();

        // Clear data-oriented storage
        self.block_metadata.ids.clearRetainingCapacity();
        self.block_metadata.versions.clearRetainingCapacity();
        self.block_metadata.sizes.clearRetainingCapacity();
        self.block_metadata.timestamps.clearRetainingCapacity();

        // Reset metrics
        self.allocation_metrics = AllocationMetrics.init();

        std.log.info("Subsystem reset complete - O(1) bulk cleanup", .{});
    }

    /// Deinitialize integrated memory system.
    pub fn deinit(self: *Self) void {
        self.allocation_metrics.report();

        self.query_session.invalidate();
        self.block_metadata.deinit();
        self.sstable_pool.deinit();
        self.iterator_pool.deinit();
        self.subsystem_arena.deinit();
        self.backing_allocator.destroy(self.subsystem_arena);

        std.log.info("Integrated memory system deinitialized", .{});
    }
};

/// Performance benchmark results
pub const PerformanceBenchmark = struct {
    total_time_ns: u64,
    operations_per_second: f64,
    ns_per_operation: u64,
    memory_efficiency: f64,

    /// Report benchmark results to standard logging.
    /// Displays performance metrics including throughput and latency measurements.
    pub fn report(self: *const PerformanceBenchmark) void {
        std.log.info("=== Performance Benchmark Results ===", .{});
        std.log.info("Operations per second: {d:.0}", .{self.operations_per_second});
        std.log.info("Nanoseconds per operation: {}", .{self.ns_per_operation});
        std.log.info("Memory efficiency: {d:.1}%", .{self.memory_efficiency * 100.0});
        std.log.info("Total benchmark time: {d:.2}ms", .{@as(f64, @floatFromInt(self.total_time_ns)) / 1_000_000.0});
    }
};

// Tests demonstrating the integrated memory system

const testing = std.testing;

test "IntegratedMemorySystem initialization and basic operations" {
    var system = try IntegratedMemorySystem.init(testing.allocator);
    defer system.deinit();

    // Test that all components are properly initialized
    try testing.expect(system.sstable_pool.capacity() == 32);
    try testing.expect(system.iterator_pool.capacity() == 64);
    try testing.expect(!system.temp_buffer_pool.is_exhausted());
    try testing.expect(system.query_session.is_active);
}

test "IntegratedMemorySystem memory lifecycle demonstration" {
    var system = try IntegratedMemorySystem.init(testing.allocator);
    defer system.deinit();

    // This should not fail and should demonstrate all memory patterns
    try system.demonstrate_memory_lifecycle();

    // Verify metrics were updated
    try testing.expect(system.allocation_metrics.pool_acquisitions > 0);
    try testing.expect(system.allocation_metrics.arena_allocations > 0);
    try testing.expect(system.allocation_metrics.zero_copy_accesses > 0);
}

test "IntegratedMemorySystem performance benchmarking" {
    var system = try IntegratedMemorySystem.init(testing.allocator);
    defer system.deinit();

    const benchmark = try system.benchmark_performance(1000);
    benchmark.report();

    // Use tiered performance assertions for cross-platform compatibility
    const tier = PerformanceAssertion.PerformanceTier.detect();
    const thresholds = PerformanceAssertion.PerformanceThresholds.for_tier(10000, 100000, tier); // 10μs base, 100K ops/sec base

    try testing.expect(benchmark.ns_per_operation < thresholds.max_latency_ns);
    try testing.expect(benchmark.operations_per_second > @as(f64, @floatFromInt(thresholds.min_throughput_ops_per_sec)));
}

test "IntegratedMemorySystem reset and reuse" {
    var system = try IntegratedMemorySystem.init(testing.allocator);
    defer system.deinit();

    // Use the system
    try system.demonstrate_memory_lifecycle();

    const initial_allocations = system.allocation_metrics.arena_allocations;
    try testing.expect(initial_allocations > 0);

    // Reset everything
    system.reset_subsystems();

    // Verify reset worked
    try testing.expect(system.allocation_metrics.arena_allocations == 0);
    try testing.expect(system.sstable_pool.active_count() == 0);
    try testing.expect(system.temp_buffer_pool.active_count() == 0);

    // Should be able to use again
    try system.demonstrate_memory_lifecycle();
}
