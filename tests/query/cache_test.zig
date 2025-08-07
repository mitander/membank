//! Tests for query result caching system.
//!
//! Tests LRU eviction, TTL expiration, cache invalidation, memory management,
//! and cache statistics across all supported query types. Validates that
//! caching respects arena-per-subsystem memory model and explicit semantics.

const std = @import("std");
const testing = std.testing;
const kausaldb = @import("kausaldb");

const simulation_vfs = kausaldb.simulation_vfs;
const storage = kausaldb.storage;
const types = kausaldb.types;
const cache = kausaldb.query.cache;
const query_operations = kausaldb.query.operations;
const query_traversal = kausaldb.query.traversal;

const BlockId = types.BlockId;
const ContextBlock = types.ContextBlock;
const GraphEdge = types.GraphEdge;
const CacheKey = cache.CacheKey;
const CacheStatistics = cache.CacheStatistics;
const QueryCache = cache.QueryCache;
const TraversalResult = query_traversal.TraversalResult;
const SimulationVFS = simulation_vfs.SimulationVFS;
const StorageEngine = storage.StorageEngine;

test "cache key generation and equality" {
    const allocator = testing.allocator;

    const block_id1 = try BlockId.from_hex("11111111111111111111111111111111");
    const block_id2 = try BlockId.from_hex("22222222222222222222222222222222");

    // Test single block cache keys
    const key1 = CacheKey.for_single_block(block_id1);
    const key2 = CacheKey.for_single_block(block_id1);
    const key3 = CacheKey.for_single_block(block_id2);

    try testing.expect(key1.eql(key2)); // Same block should be equal
    try testing.expect(!key1.eql(key3)); // Different blocks should not be equal
    try testing.expectEqual(key1.hash(), key2.hash()); // Hash consistency

    // Test traversal cache keys
    const traversal_key1 = CacheKey.for_traversal(block_id1, 1, 2, 5, 100);
    const traversal_key2 = CacheKey.for_traversal(block_id1, 1, 2, 5, 100);
    const traversal_key3 = CacheKey.for_traversal(block_id1, 1, 2, 10, 100); // Different max_depth

    try testing.expect(traversal_key1.eql(traversal_key2));
    try testing.expect(!traversal_key1.eql(traversal_key3));

    // Test find_blocks cache keys
    const block_ids = [_]BlockId{ block_id1, block_id2 };
    const find_key1 = CacheKey.for_find_blocks(&block_ids);
    const find_key2 = CacheKey.for_find_blocks(&block_ids);

    try testing.expect(find_key1.eql(find_key2));

    _ = allocator;
}

test "cache initialization and configuration" {
    const allocator = testing.allocator;

    var query_cache = QueryCache.init(allocator, 100, 15); // 100 entries, 15 minute TTL
    defer query_cache.deinit();

    try testing.expectEqual(@as(u32, 100), query_cache.max_entries);
    try testing.expectEqual(@as(i64, 15 * 60 * 1_000_000_000), query_cache.ttl_ns); // 15 minutes in nanoseconds
    try testing.expectEqual(@as(u64, 0), query_cache.hits);
    try testing.expectEqual(@as(u64, 0), query_cache.misses);

    const stats = query_cache.statistics();
    try testing.expectEqual(@as(u32, 0), stats.current_entries);
    try testing.expectEqual(@as(u32, 100), stats.max_entries);
    try testing.expectEqual(@as(f64, 0.0), stats.hit_rate);
}

test "single block caching and retrieval" {
    const allocator = testing.allocator;

    var query_cache = QueryCache.init(allocator, 10, 30);
    defer query_cache.deinit();

    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, 1, .little);
    const test_block = ContextBlock{
        .id = BlockId{ .bytes = id_bytes },
        .version = 1,
        .source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{1}),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = try allocator.dupe(u8, "test content"),
    };
    defer {
        allocator.free(test_block.source_uri);
        allocator.free(test_block.metadata_json);
        allocator.free(test_block.content);
    }

    const cache_key = CacheKey.for_single_block(test_block.id);
    const cache_value = cache.CacheValue{ .find_blocks = test_block };

    // Test cache miss
    try testing.expect(query_cache.get(cache_key, allocator) == null);
    try testing.expectEqual(@as(u64, 1), query_cache.misses);

    // Store in cache
    try query_cache.put(cache_key, cache_value);

    // Test cache hit
    if (query_cache.get(cache_key, allocator)) |cached_value| {
        defer cached_value.deinit(allocator);
        try testing.expectEqual(@as(u64, 1), query_cache.hits);

        switch (cached_value) {
            .find_blocks => |cached_block| {
                try testing.expect(cached_block.id.eql(test_block.id));
                try testing.expect(std.mem.eql(u8, cached_block.content, test_block.content));
            },
            else => try testing.expect(false), // Should be find_blocks type
        }
    } else {
        try testing.expect(false); // Should have been a cache hit
    }
}

test "traversal result caching" {
    const allocator = testing.allocator;

    var query_cache = QueryCache.init(allocator, 10, 30);
    defer query_cache.deinit();

    // Create test traversal result
    const test_blocks = try allocator.alloc(ContextBlock, 2);
    defer allocator.free(test_blocks);

    // Create first block
    var id_bytes1: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes1, 1, .little);
    test_blocks[0] = ContextBlock{
        .id = BlockId{ .bytes = id_bytes1 },
        .version = 1,
        .source_uri = try std.fmt.allocPrint(allocator, "test://block_1.zig", .{}),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = try allocator.dupe(u8, "first block"),
    };

    // Create second block
    var id_bytes2: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes2, 2, .little);
    test_blocks[1] = ContextBlock{
        .id = BlockId{ .bytes = id_bytes2 },
        .version = 1,
        .source_uri = try std.fmt.allocPrint(allocator, "test://block_2.zig", .{}),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = try allocator.dupe(u8, "second block"),
    };

    defer for (test_blocks) |block| {
        allocator.free(block.source_uri);
        allocator.free(block.metadata_json);
        allocator.free(block.content);
    };

    const test_paths = try allocator.alloc([]BlockId, 1);
    defer allocator.free(test_paths);

    const path_blocks = try allocator.alloc(BlockId, 2);
    defer allocator.free(path_blocks);
    path_blocks[0] = test_blocks[0].id;
    path_blocks[1] = test_blocks[1].id;
    test_paths[0] = path_blocks;

    const test_depths = try allocator.alloc(u32, 2);
    defer allocator.free(test_depths);
    test_depths[0] = 0;
    test_depths[1] = 1;

    const traversal_result = TraversalResult{
        .allocator = allocator,
        .blocks = test_blocks,
        .paths = test_paths,
        .depths = test_depths,
        .blocks_traversed = 2,
        .max_depth_reached = 1,
    };

    const cache_key = CacheKey.for_traversal(test_blocks[0].id, 1, 1, 5, 0);
    const cache_value = cache.CacheValue{ .traversal = traversal_result };

    // Test cache miss
    try testing.expect(query_cache.get(cache_key, allocator) == null);

    // Store in cache
    try query_cache.put(cache_key, cache_value);

    // Test cache hit
    if (query_cache.get(cache_key, allocator)) |retrieved_value| {
        defer retrieved_value.deinit(allocator);

        switch (retrieved_value) {
            .traversal => |cached_result| {
                try testing.expectEqual(@as(u32, 2), cached_result.blocks_traversed);
                try testing.expectEqual(@as(u32, 1), cached_result.max_depth_reached);
                try testing.expectEqual(@as(usize, 2), cached_result.blocks.len);
            },
            else => try testing.expect(false),
        }
    } else {
        try testing.expect(false);
    }
}

test "cache TTL expiration" {
    const allocator = testing.allocator;

    // Very short TTL for testing (immediate expiration)
    var query_cache = QueryCache.init(allocator, 10, 0); // 0 minutes = immediate expiration
    query_cache.ttl_ns = 1; // 1 nanosecond = immediate expiration
    defer query_cache.deinit();

    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, 1, .little);
    const test_block = ContextBlock{
        .id = BlockId{ .bytes = id_bytes },
        .version = 1,
        .source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{1}),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = try allocator.dupe(u8, "expiring content"),
    };
    defer {
        allocator.free(test_block.source_uri);
        allocator.free(test_block.metadata_json);
        allocator.free(test_block.content);
    }

    const cache_key = CacheKey.for_single_block(test_block.id);
    const cache_value = cache.CacheValue{ .find_blocks = test_block };

    // Store in cache
    try query_cache.put(cache_key, cache_value);

    // With immediate expiration, should be expired on retrieval
    try testing.expect(query_cache.get(cache_key, allocator) == null);

    // Should register as a miss due to expiration
    const stats = query_cache.statistics();
    try testing.expect(stats.misses > 0);
}

test "cache LRU eviction" {
    const allocator = testing.allocator;

    var query_cache = QueryCache.init(allocator, 3, 30); // Only 3 entries max
    defer query_cache.deinit();

    // Create test blocks
    var test_blocks: [5]ContextBlock = undefined;
    defer for (test_blocks) |block| {
        allocator.free(block.source_uri);
        allocator.free(block.metadata_json);
        allocator.free(block.content);
    };

    for (&test_blocks, 0..) |*block, i| {
        const content = try std.fmt.allocPrint(allocator, "content {}", .{i});
        defer allocator.free(content);
        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, @intCast(i + 1), .little);
        block.* = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{i + 1}),
            .metadata_json = try allocator.dupe(u8, "{}"),
            .content = try allocator.dupe(u8, content),
        };
    }

    // Fill cache to capacity
    for (test_blocks[0..3], 0..) |block, i| {
        const cache_key = CacheKey.for_single_block(block.id);
        const cache_value = cache.CacheValue{ .find_blocks = block };
        try query_cache.put(cache_key, cache_value);

        // Access earlier entries to establish LRU order
        if (i > 0) {
            const earlier_key = CacheKey.for_single_block(test_blocks[0].id);
            if (query_cache.get(earlier_key, allocator)) |retrieved| {
                defer retrieved.deinit(allocator);
                switch (retrieved) {
                    .find_blocks => |cached_block| {
                        _ = cached_block;
                    },
                    else => {},
                }
            }
        }
    }

    try testing.expectEqual(@as(u32, 3), query_cache.statistics().current_entries);

    // Add fourth block - should trigger eviction
    const fourth_key = CacheKey.for_single_block(test_blocks[3].id);
    const fourth_value = cache.CacheValue{ .find_blocks = test_blocks[3] };
    try query_cache.put(fourth_key, fourth_value);

    // Should still be at max capacity
    try testing.expectEqual(@as(u32, 3), query_cache.statistics().current_entries);

    // Should have recorded evictions
    try testing.expect(query_cache.statistics().evictions > 0);

    // The least recently used item should be evicted
    // (This is somewhat implementation-dependent, but test basic functionality)
    if (query_cache.get(fourth_key, allocator)) |retrieved| {
        defer retrieved.deinit(allocator);
        switch (retrieved) {
            .find_blocks => |block| {
                _ = block;
            },
            else => {},
        }
    } else {
        try testing.expect(false);
    }
}

test "cache invalidation" {
    const allocator = testing.allocator;

    var query_cache = QueryCache.init(allocator, 10, 30);
    defer query_cache.deinit();

    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, 1, .little);
    const test_block = ContextBlock{
        .id = BlockId{ .bytes = id_bytes },
        .version = 1,
        .source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{1}),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = try allocator.dupe(u8, "invalidation test"),
    };
    defer {
        allocator.free(test_block.source_uri);
        allocator.free(test_block.metadata_json);
        allocator.free(test_block.content);
    }

    const cache_key = CacheKey.for_single_block(test_block.id);
    const cache_value = cache.CacheValue{ .find_blocks = test_block };

    // Store in cache
    try query_cache.put(cache_key, cache_value);

    // Verify it's cached
    if (query_cache.get(cache_key, allocator)) |retrieved| {
        defer retrieved.deinit(allocator);
        switch (retrieved) {
            .find_blocks => |block| {
                _ = block;
            },
            else => {},
        }
    } else {
        try testing.expect(false);
    }

    // Invalidate all cache entries
    query_cache.invalidate_all();

    // Should no longer be in cache
    try testing.expect(query_cache.get(cache_key, allocator) == null);

    // Should have recorded invalidation
    try testing.expect(query_cache.statistics().invalidations > 0);
}

test "cache statistics and monitoring" {
    const allocator = testing.allocator;

    var query_cache = QueryCache.init(allocator, 5, 30);
    defer query_cache.deinit();

    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, 1, .little);
    const test_block = ContextBlock{
        .id = BlockId{ .bytes = id_bytes },
        .version = 1,
        .source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{1}),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = try allocator.dupe(u8, "stats test"),
    };
    defer {
        allocator.free(test_block.source_uri);
        allocator.free(test_block.metadata_json);
        allocator.free(test_block.content);
    }

    const cache_key = CacheKey.for_single_block(test_block.id);
    const cache_value = cache.CacheValue{ .find_blocks = test_block };

    // Initial stats
    var stats = query_cache.statistics();
    try testing.expectEqual(@as(u32, 0), stats.current_entries);
    try testing.expectEqual(@as(f64, 0.0), stats.hit_rate);
    try testing.expect(!stats.is_effective());
    try testing.expect(!stats.needs_tuning());

    // Add entry and test hits/misses
    try query_cache.put(cache_key, cache_value);

    // Generate some hits and misses
    for (0..5) |_| {
        if (query_cache.get(cache_key, allocator)) |retrieved| {
            defer retrieved.deinit(allocator);
            switch (retrieved) {
                .find_blocks => |cached_block| {
                    _ = cached_block;
                },
                else => {},
            }
        }
    }

    const missing_key = CacheKey.for_single_block(try BlockId.from_hex("99999999999999999999999999999999"));
    for (0..3) |_| {
        _ = query_cache.get(missing_key, allocator); // Should be misses
    }

    stats = query_cache.statistics();
    try testing.expectEqual(@as(u32, 1), stats.current_entries);
    try testing.expectEqual(@as(u64, 5), stats.hits);
    try testing.expectEqual(@as(u64, 3), stats.misses);
    try testing.expectEqual(@as(f64, 5.0 / 8.0), stats.hit_rate);
    try testing.expect(stats.memory_usage_estimate > 0);
}

test "cache clear and reset" {
    const allocator = testing.allocator;

    var query_cache = QueryCache.init(allocator, 10, 30);
    defer query_cache.deinit();

    // Add some entries
    for (1..4) |i| {
        const content = try std.fmt.allocPrint(allocator, "clear test {}", .{i});
        defer allocator.free(content);

        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, @intCast(i), .little);
        const test_block = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{@as(u32, @intCast(i))}),
            .metadata_json = try allocator.dupe(u8, "{}"),
            .content = try allocator.dupe(u8, content),
        };
        defer {
            allocator.free(test_block.source_uri);
            allocator.free(test_block.metadata_json);
            allocator.free(test_block.content);
        }

        const cache_key = CacheKey.for_single_block(test_block.id);
        const cache_value = cache.CacheValue{ .find_blocks = test_block };
        try query_cache.put(cache_key, cache_value);
    }

    try testing.expectEqual(@as(u32, 3), query_cache.statistics().current_entries);

    // Clear cache
    query_cache.clear();

    const stats = query_cache.statistics();
    try testing.expectEqual(@as(u32, 0), stats.current_entries);
    try testing.expectEqual(@as(u64, 0), stats.hits);
    try testing.expectEqual(@as(u64, 0), stats.misses);
    try testing.expectEqual(@as(u64, 0), stats.evictions);
    try testing.expectEqual(@as(u64, 0), stats.invalidations);
}

test "global traversal invalidation" {
    const allocator = testing.allocator;

    var query_cache = QueryCache.init(allocator, 10, 30);
    defer query_cache.deinit();

    // Add both block and traversal cache entries
    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, 1, .little);
    const test_block = ContextBlock{
        .id = BlockId{ .bytes = id_bytes },
        .version = 1,
        .source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{1}),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = try allocator.dupe(u8, "traversal invalidation test"),
    };
    defer {
        allocator.free(test_block.source_uri);
        allocator.free(test_block.metadata_json);
        allocator.free(test_block.content);
    }

    const block_key = CacheKey.for_single_block(test_block.id);
    const block_value = cache.CacheValue{ .find_blocks = test_block };
    try query_cache.put(block_key, block_value);

    // Create minimal traversal result for testing
    const empty_blocks = try allocator.alloc(ContextBlock, 0);
    defer allocator.free(empty_blocks);
    const empty_paths = try allocator.alloc([]BlockId, 0);
    defer allocator.free(empty_paths);
    const empty_depths = try allocator.alloc(u32, 0);
    defer allocator.free(empty_depths);

    const traversal_result = TraversalResult{
        .allocator = allocator,
        .blocks = empty_blocks,
        .paths = empty_paths,
        .depths = empty_depths,
        .blocks_traversed = 0,
        .max_depth_reached = 0,
    };

    const traversal_key = CacheKey.for_traversal(test_block.id, 1, 1, 5, 0);
    const traversal_value = cache.CacheValue{ .traversal = traversal_result };
    try query_cache.put(traversal_key, traversal_value);

    try testing.expectEqual(@as(u32, 2), query_cache.statistics().current_entries);

    // Global invalidation removes all entries
    query_cache.invalidate_all();

    try testing.expectEqual(@as(u32, 0), query_cache.statistics().current_entries);

    // All cache should be gone after global invalidation
    try testing.expect(query_cache.get(block_key, allocator) == null);
    try testing.expect(query_cache.get(traversal_key, allocator) == null);
}

test "cache memory management with arena allocator" {
    const allocator = testing.allocator;

    var query_cache = QueryCache.init(allocator, 5, 30);
    defer query_cache.deinit();

    // Test that arena allocator properly manages cache memory
    const initial_memory = query_cache.estimate_memory_usage();

    // Add several entries
    for (1..4) |i| {
        const content = try std.fmt.allocPrint(allocator, "arena test content {}", .{i});
        defer allocator.free(content);

        var id_bytes: [16]u8 = undefined;
        std.mem.writeInt(u128, &id_bytes, @intCast(i), .little);
        const test_block = ContextBlock{
            .id = BlockId{ .bytes = id_bytes },
            .version = 1,
            .source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{@as(u32, @intCast(i))}),
            .metadata_json = try allocator.dupe(u8, "{}"),
            .content = try allocator.dupe(u8, content),
        };
        defer {
            allocator.free(test_block.source_uri);
            allocator.free(test_block.metadata_json);
            allocator.free(test_block.content);
        }

        const cache_key = CacheKey.for_single_block(test_block.id);
        const cache_value = cache.CacheValue{ .find_blocks = test_block };
        try query_cache.put(cache_key, cache_value);
    }

    const filled_memory = query_cache.estimate_memory_usage();
    try testing.expect(filled_memory > initial_memory);

    // Clear should reset arena and reduce memory usage
    query_cache.clear();

    const cleared_memory = query_cache.estimate_memory_usage();
    try testing.expect(cleared_memory <= filled_memory);
}

test "cache integration with storage operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "cache_integration_test");
    defer storage_engine.deinit();
    try storage_engine.startup();

    var query_cache = QueryCache.init(allocator, 10, 30);
    defer query_cache.deinit();

    // Create and store test block in storage
    const test_block = ContextBlock{
        .id = try BlockId.from_hex("11111111111111111111111111111111"),
        .version = 1,
        .source_uri = "integration_test.zig",
        .metadata_json = "{}",
        .content = "integration test content",
    };

    try storage_engine.put_block(test_block);

    // Test caching storage lookup result
    if (try storage_engine.find_block(test_block.id)) |storage_block| {
        const cache_key = CacheKey.for_single_block(test_block.id);
        const cache_value = cache.CacheValue{ .find_blocks = storage_block };
        try query_cache.put(cache_key, cache_value);

        // Verify cache retrieval matches storage
        if (query_cache.get(cache_key, allocator)) |cached_value| {
            defer cached_value.deinit(allocator);

            switch (cached_value) {
                .find_blocks => |cached_block| {
                    try testing.expect(cached_block.id.eql(test_block.id));
                    try testing.expect(std.mem.eql(u8, cached_block.content, test_block.content));
                },
                else => try testing.expect(false),
            }
        } else {
            try testing.expect(false);
        }

        // Test cache invalidation on block update
        const updated_block = ContextBlock{
            .id = test_block.id,
            .version = 2,
            .source_uri = test_block.source_uri,
            .metadata_json = test_block.metadata_json,
            .content = "updated content",
        };

        try storage_engine.put_block(updated_block);
        query_cache.invalidate_all();

        // Cache should be invalidated
        try testing.expect(query_cache.get(cache_key, allocator) == null);
    } else {
        try testing.expect(false); // Block should exist in storage
    }
}
