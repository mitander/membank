//! Query result caching system for KausalDB.
//!
//! Provides LRU-based query result caching to reduce expensive storage lookups
//! and traversals. Follows arena-per-subsystem memory model with explicit
//! cache invalidation on data mutations for correctness guarantees.

const std = @import("std");
const assert = @import("../core/assert.zig").assert;
const assert_fmt = @import("../core/assert.zig").assert_fmt;
const context_block = @import("../core/types.zig");
const operations = @import("operations.zig");
const traversal = @import("traversal.zig");

const ContextBlock = context_block.ContextBlock;
const BlockId = context_block.BlockId;
const QueryResult = operations.QueryResult;
const TraversalResult = traversal.TraversalResult;

/// Cache key for query results - designed for equality comparison and hashing
pub const CacheKey = struct {
    query_type: QueryType,
    primary_block_id: BlockId,
    parameters_hash: u64,

    pub const QueryType = enum(u8) {
        find_blocks = 1,
        traversal = 2,
        semantic = 3,
        filtered = 4,
    };

    /// Create cache key for single block lookup
    pub fn for_single_block(block_id: BlockId) CacheKey {
        return CacheKey{
            .query_type = .find_blocks,
            .primary_block_id = block_id,
            .parameters_hash = 0, // Single block has no additional parameters
        };
    }

    /// Create cache key for find_blocks query (currently not used for caching due to streaming nature)
    pub fn for_find_blocks(block_ids: []const BlockId) CacheKey {
        var hasher = std.hash.Wyhash.init(0);
        for (block_ids) |id| {
            hasher.update(&id.bytes);
        }

        return CacheKey{
            .query_type = .find_blocks,
            .primary_block_id = if (block_ids.len > 0) block_ids[0] else BlockId.from_bytes([_]u8{0} ** 16),
            .parameters_hash = hasher.final(),
        };
    }

    /// Create cache key for traversal query
    pub fn for_traversal(
        start_id: BlockId,
        direction: u8,
        algorithm: u8,
        max_depth: u32,
        edge_filter_hash: u64,
    ) CacheKey {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(&[_]u8{direction});
        hasher.update(&[_]u8{algorithm});
        hasher.update(std.mem.asBytes(&max_depth));
        hasher.update(std.mem.asBytes(&edge_filter_hash));

        return CacheKey{
            .query_type = .traversal,
            .primary_block_id = start_id,
            .parameters_hash = hasher.final(),
        };
    }

    /// Hash function for cache key
    pub fn hash(self: CacheKey) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(&[_]u8{@intFromEnum(self.query_type)});
        hasher.update(&self.primary_block_id.bytes);
        hasher.update(std.mem.asBytes(&self.parameters_hash));
        return hasher.final();
    }

    /// Equality comparison
    pub fn eql(self: CacheKey, other: CacheKey) bool {
        return self.query_type == other.query_type and
            self.primary_block_id.eql(other.primary_block_id) and
            self.parameters_hash == other.parameters_hash;
    }
};

/// Cached query result with metadata for LRU management
const CacheEntry = struct {
    key: CacheKey,
    result: CacheValue,
    access_count: u32,
    last_access_time_ns: i64,
    creation_time_ns: i64,

    /// Mark this entry as accessed for LRU tracking
    pub fn mark_accessed(self: *CacheEntry) void {
        self.access_count += 1;
        self.last_access_time_ns = @intCast(std.time.nanoTimestamp());
    }

    /// Check if entry is expired based on TTL
    pub fn is_expired(self: *const CacheEntry, ttl_ns: i64) bool {
        const current_time: i64 = @intCast(std.time.nanoTimestamp());
        return (current_time - self.creation_time_ns) > ttl_ns;
    }
};

/// Union type for different cached result types - focusing on expensive operations
pub const CacheValue = union(CacheKey.QueryType) {
    find_blocks: ContextBlock, // Cache individual blocks rather than streaming results
    traversal: TraversalResult,
    semantic: void, // Placeholder for future semantic query results
    filtered: void, // Placeholder for future filtered query results

    /// Clone the cache value using caller's allocator
    pub fn clone(self: *const CacheValue, allocator: std.mem.Allocator) !CacheValue {
        switch (self.*) {
            .find_blocks => |block| {
                return CacheValue{ .find_blocks = try clone_block(allocator, block) };
            },
            .traversal => |result| {
                return CacheValue{ .traversal = try result.clone(allocator) };
            },
            .semantic, .filtered => return self.*,
        }
    }

    /// Free memory used by cached value
    pub fn deinit(self: CacheValue, allocator: std.mem.Allocator) void {
        switch (self) {
            .find_blocks => |block| {
                allocator.free(block.source_uri);
                allocator.free(block.metadata_json);
                allocator.free(block.content);
            },
            .traversal => |result| {
                result.deinit();
            },
            .semantic, .filtered => {},
        }
    }
};

/// Clone a context block for caching or returning from cache
fn clone_block(allocator: std.mem.Allocator, block: ContextBlock) !ContextBlock {
    return ContextBlock{
        .id = block.id,
        .version = block.version,
        .source_uri = try allocator.dupe(u8, block.source_uri),
        .metadata_json = try allocator.dupe(u8, block.metadata_json),
        .content = try allocator.dupe(u8, block.content),
    };
}

/// Hash context for CacheKey
const CacheKeyHashContext = struct {
    pub fn hash(ctx: @This(), key: CacheKey) u64 {
        _ = ctx;
        return key.hash();
    }
    pub fn eql(ctx: @This(), a: CacheKey, b: CacheKey) bool {
        _ = ctx;
        return a.eql(b);
    }
};

/// Query result cache with LRU eviction and invalidation support
pub const QueryCache = struct {
    arena: std.heap.ArenaAllocator,
    cache: std.HashMap(CacheKey, CacheEntry, CacheKeyHashContext, 80),

    max_entries: u32,
    ttl_ns: i64,

    hits: u64,
    misses: u64,
    evictions: u64,
    invalidations: u64,

    /// Default cache configuration values
    pub const DEFAULT_MAX_ENTRIES: u32 = 1000;
    pub const DEFAULT_TTL_MINUTES: u32 = 30;

    /// Initialize query cache with arena allocator
    pub fn init(backing_allocator: std.mem.Allocator, max_entries: u32, ttl_minutes: u32) QueryCache {
        return QueryCache{
            .arena = std.heap.ArenaAllocator.init(backing_allocator),
            .cache = std.HashMap(CacheKey, CacheEntry, CacheKeyHashContext, 80).init(backing_allocator),
            .max_entries = max_entries,
            .ttl_ns = @as(i64, ttl_minutes) * 60 * 1_000_000_000, // Convert minutes to nanoseconds
            .hits = 0,
            .misses = 0,
            .evictions = 0,
            .invalidations = 0,
        };
    }

    /// Clean up cache resources
    pub fn deinit(self: *QueryCache) void {
        self.cache.deinit();
        self.arena.deinit();
    }

    /// Clear all cached entries and reset statistics
    pub fn clear(self: *QueryCache) void {
        self.cache.clearRetainingCapacity();
        _ = self.arena.reset(.retain_capacity); // Arena handles all value cleanup

        self.hits = 0;
        self.misses = 0;
        self.evictions = 0;
        self.invalidations = 0;
    }

    /// Get cached result for query key
    pub fn get(self: *QueryCache, key: CacheKey, allocator: std.mem.Allocator) ?CacheValue {
        if (self.cache.getPtr(key)) |entry| {
            if (entry.is_expired(self.ttl_ns)) {
                _ = self.cache.remove(key);
                self.misses += 1;
                return null;
            }

            entry.mark_accessed();
            self.hits += 1;
            return entry.result.clone(allocator) catch null;
        }

        self.misses += 1;
        return null;
    }

    /// Store query result in cache
    pub fn put(self: *QueryCache, key: CacheKey, value: CacheValue) !void {
        if (self.cache.count() >= self.max_entries) {
            try self.evict_lru_entries();
        }

        const arena_allocator = self.arena.allocator();
        const cached_value = try value.clone(arena_allocator);

        const entry = CacheEntry{
            .key = key,
            .result = cached_value,
            .access_count = 1,
            .last_access_time_ns = @intCast(std.time.nanoTimestamp()),
            .creation_time_ns = @intCast(std.time.nanoTimestamp()),
        };

        try self.cache.put(key, entry);
    }

    /// Simple cache invalidation - just clear everything for simplicity
    pub fn invalidate_all(self: *QueryCache) void {
        self.cache.clearRetainingCapacity();
        _ = self.arena.reset(.retain_capacity); // Arena handles all value cleanup

        self.invalidations += 1;
    }

    /// Evict least recently used entries to make space
    fn evict_lru_entries(self: *QueryCache) !void {
        const target_size = (self.max_entries * 3) / 4; // Evict 25% of entries

        var eviction_candidates = try std.ArrayList(struct {
            key: CacheKey,
            last_access: i64,
            access_count: u32,
        }).initCapacity(self.arena.allocator(), self.cache.count());
        defer eviction_candidates.deinit();

        var iterator = self.cache.iterator();
        while (iterator.next()) |entry| {
            try eviction_candidates.append(.{
                .key = entry.key_ptr.*,
                .last_access = entry.value_ptr.last_access_time_ns,
                .access_count = entry.value_ptr.access_count,
            });
        }

        std.sort.pdq(
            @TypeOf(eviction_candidates.items[0]),
            eviction_candidates.items,
            {},
            struct {
                fn less_than(
                    context: void,
                    a: @TypeOf(eviction_candidates.items[0]),
                    b: @TypeOf(eviction_candidates.items[0]),
                ) bool {
                    _ = context;
                    if (a.last_access != b.last_access) {
                        return a.last_access < b.last_access;
                    }
                    return a.access_count < b.access_count;
                }
            }.less_than,
        );

        const entries_to_evict = self.cache.count() - target_size;
        for (eviction_candidates.items[0..entries_to_evict]) |candidate| {
            if (self.cache.getPtr(candidate.key)) |_| {
                _ = self.cache.remove(candidate.key);
                self.evictions += 1;
            }
        }
    }

    /// Get cache statistics
    pub fn statistics(self: *const QueryCache) CacheStatistics {
        const total_requests = self.hits + self.misses;
        const hit_rate = if (total_requests > 0) @as(f64, @floatFromInt(self.hits)) / @as(f64, @floatFromInt(total_requests)) else 0.0;

        return CacheStatistics{
            .current_entries = @intCast(self.cache.count()),
            .max_entries = self.max_entries,
            .hits = self.hits,
            .misses = self.misses,
            .hit_rate = hit_rate,
            .evictions = self.evictions,
            .invalidations = self.invalidations,
            .memory_usage_estimate = self.estimate_memory_usage(),
        };
    }

    /// Estimate cache memory usage for monitoring
    pub fn estimate_memory_usage(self: *const QueryCache) u64 {
        const cache_overhead = self.cache.count() * (@sizeOf(CacheKey) + @sizeOf(CacheEntry));

        return cache_overhead + @as(u64, @intCast(self.cache.count() * 1024)); // Estimate 1KB per cached result
    }
};

/// Cache performance and usage statistics
pub const CacheStatistics = struct {
    current_entries: u32,
    max_entries: u32,
    hits: u64,
    misses: u64,
    hit_rate: f64,
    evictions: u64,
    invalidations: u64,
    memory_usage_estimate: u64,

    /// Check if cache is performing well
    pub fn is_effective(self: *const CacheStatistics) bool {
        return self.hit_rate > 0.2 and self.current_entries > (self.max_entries / 4);
    }

    /// Check if cache needs tuning (too many evictions)
    pub fn needs_tuning(self: *const CacheStatistics) bool {
        const total_operations = self.hits + self.misses;
        if (total_operations == 0) return false;

        const eviction_rate = @as(f64, @floatFromInt(self.evictions)) / @as(f64, @floatFromInt(total_operations));
        return eviction_rate > 0.1;
    }
};
