//! Query result caching system for Membank.
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

/// Cache key hash context for BlockId operations
const BlockIdHashContext = struct {
    pub fn hash(ctx: @This(), key: BlockId) u64 {
        _ = ctx;
        return std.hash_map.hashString(&key.bytes);
    }
    pub fn eql(ctx: @This(), a: BlockId, b: BlockId) bool {
        _ = ctx;
        return a.eql(b);
    }
};

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
        // Simple hash of all block IDs for multi-block queries
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
    pub fn for_traversal(start_id: BlockId, direction: u8, algorithm: u8, max_depth: u32, edge_type: ?u16) CacheKey {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(&[_]u8{direction});
        hasher.update(&[_]u8{algorithm});
        hasher.update(std.mem.asBytes(&max_depth));
        if (edge_type) |et| {
            hasher.update(std.mem.asBytes(&et));
        }
        
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

    /// Clone the cache value for return to caller
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
    pub fn deinit(self: CacheValue) void {
        switch (self) {
            .find_blocks => {
                // Note: Individual blocks use the cache's arena allocator,
                // so they'll be freed when arena is reset
            },
            .traversal => |result| {
                // Individual traversal results will be freed with arena reset
                _ = result;
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
    invalidation_set: std.HashMap(BlockId, void, BlockIdHashContext, 80),
    
    // Cache configuration
    max_entries: u32,
    ttl_ns: i64,
    
    // Cache statistics
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
            .invalidation_set = std.HashMap(BlockId, void, BlockIdHashContext, 80).init(backing_allocator),
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
        // Free all cached values
        var iterator = self.cache.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.result.deinit();
        }
        
        self.cache.deinit();
        self.invalidation_set.deinit();
        self.arena.deinit();
    }

    /// Clear all cached entries and reset statistics
    pub fn clear(self: *QueryCache) void {
        // Free all cached values before clearing
        var iterator = self.cache.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.result.deinit();
        }
        
        self.cache.clearRetainingCapacity();
        self.invalidation_set.clearRetainingCapacity();
        _ = self.arena.reset(.retain_capacity);
        
        // Reset statistics but preserve configuration
        self.hits = 0;
        self.misses = 0;
        self.evictions = 0;
        self.invalidations = 0;
    }

    /// Get cached result for query key
    pub fn get(self: *QueryCache, key: CacheKey, allocator: std.mem.Allocator) ?CacheValue {
        if (self.cache.getPtr(key)) |entry| {
            // Check if entry is expired
            if (entry.is_expired(self.ttl_ns)) {
                // Remove expired entry
                entry.result.deinit();
                _ = self.cache.remove(key);
                self.misses += 1;
                return null;
            }

            // Update access tracking for LRU
            entry.mark_accessed();
            self.hits += 1;

            // Clone result for caller (cache retains ownership)
            return entry.result.clone(allocator) catch null;
        }

        self.misses += 1;
        return null;
    }

    /// Store query result in cache
    pub fn put(self: *QueryCache, key: CacheKey, value: CacheValue) !void {
        // Check if we need to evict entries
        if (self.cache.count() >= self.max_entries) {
            try self.evict_lru_entries();
        }

        // Clone the value for storage in cache
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

        // Track blocks for invalidation
        try self.track_blocks_for_invalidation(key);
    }

    /// Track blocks associated with query for cache invalidation
    fn track_blocks_for_invalidation(self: *QueryCache, key: CacheKey) !void {
        // Always track the primary block
        try self.invalidation_set.put(key.primary_block_id, {});

        // For complex queries, we track additional blocks based on query type
        switch (key.query_type) {
            .find_blocks => {
                // For find_blocks, we only track the primary block ID
                // Multi-block queries use parameter hash for cache key uniqueness
            },
            .traversal => {
                // For traversal queries, we track the start block
                // Graph structure changes will require global invalidation
            },
            .semantic, .filtered => {
                // Future: implement tracking for semantic/filtered queries
            },
        }
    }

    /// Invalidate cache entries affected by block changes
    pub fn invalidate_block(self: *QueryCache, block_id: BlockId) void {
        if (!self.invalidation_set.contains(block_id)) {
            return; // No cached queries depend on this block
        }

        // Find and remove all cache entries that depend on this block
        var keys_to_remove = std.ArrayList(CacheKey).init(self.arena.allocator());
        defer keys_to_remove.deinit();
        defer keys_to_remove.deinit();

        var iterator = self.cache.iterator();
        while (iterator.next()) |entry| {
            const key = entry.key_ptr.*;
            if (key.primary_block_id.eql(block_id)) {
                keys_to_remove.append(key) catch continue;
            }
        }

        // Remove invalidated entries
        for (keys_to_remove.items) |key| {
            if (self.cache.getPtr(key)) |entry| {
                entry.result.deinit();
                _ = self.cache.remove(key);
                self.invalidations += 1;
            }
        }

        // Remove from invalidation set if no more entries depend on this block
        _ = self.invalidation_set.remove(block_id);
    }

    /// Global cache invalidation for structural changes
    pub fn invalidate_all_traversals(self: *QueryCache) void {
        var keys_to_remove = std.ArrayList(CacheKey).init(self.arena.allocator());
        defer keys_to_remove.deinit();
        defer keys_to_remove.deinit();

        var iterator = self.cache.iterator();
        while (iterator.next()) |entry| {
            if (entry.key_ptr.query_type == .traversal) {
                keys_to_remove.append(entry.key_ptr.*) catch continue;
            }
        }

        // Remove all traversal entries
        for (keys_to_remove.items) |key| {
            if (self.cache.getPtr(key)) |entry| {
                entry.result.deinit();
                _ = self.cache.remove(key);
                self.invalidations += 1;
            }
        }
    }

    /// Evict least recently used entries to make space
    fn evict_lru_entries(self: *QueryCache) !void {
        const target_size = (self.max_entries * 3) / 4; // Evict 25% of entries
        
        // Collect entries with access information for LRU calculation
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

        // Sort by LRU criteria (least recently accessed first, then by access count)
        std.sort.pdq(@TypeOf(eviction_candidates.items[0]), eviction_candidates.items, {}, struct {
            fn lessThan(context: void, a: @TypeOf(eviction_candidates.items[0]), b: @TypeOf(eviction_candidates.items[0])) bool {
                _ = context;
                if (a.last_access != b.last_access) {
                    return a.last_access < b.last_access;
                }
                return a.access_count < b.access_count;
            }
        }.lessThan);

        // Evict entries until we reach target size
        const entries_to_evict = self.cache.count() - target_size;
        for (eviction_candidates.items[0..entries_to_evict]) |candidate| {
            if (self.cache.getPtr(candidate.key)) |entry| {
                entry.result.deinit();
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
        // Rough estimate: cache structure + arena usage
        const cache_overhead = self.cache.count() * (@sizeOf(CacheKey) + @sizeOf(CacheEntry));
        const invalidation_overhead = self.invalidation_set.count() * @sizeOf(BlockId);
        
        // Arena allocator doesn't expose usage directly, so we estimate
        // This is conservative and includes overhead from allocator structures
        return cache_overhead + invalidation_overhead + @as(u64, @intCast(self.cache.count() * 1024)); // Estimate 1KB per cached result
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
        // Cache is considered effective if hit rate > 20% and we're using capacity
        return self.hit_rate > 0.2 and self.current_entries > (self.max_entries / 4);
    }

    /// Check if cache needs tuning (too many evictions)
    pub fn needs_tuning(self: *const CacheStatistics) bool {
        const total_operations = self.hits + self.misses;
        if (total_operations == 0) return false;
        
        // If evictions are more than 10% of total operations, cache may need larger size
        const eviction_rate = @as(f64, @floatFromInt(self.evictions)) / @as(f64, @floatFromInt(total_operations));
        return eviction_rate > 0.1;
    }
};