//! Bidirectional graph edge index for efficient relationship traversal.
//!
//! Maintains dual indexes (outgoing and incoming) to enable fast graph
//! traversal in both directions without requiring full edge scans.
//! Uses arena allocation for edge lists to enable O(1) bulk cleanup
//! when clearing the index. Memory management follows arena-per-subsystem
//! pattern for predictable performance and memory safety.

const std = @import("std");
const assert = @import("../core/assert.zig");
const context_block = @import("../core/types.zig");
const arena = @import("../core/arena.zig");
const ownership = @import("../core/ownership.zig");

const GraphEdge = context_block.GraphEdge;
const BlockId = context_block.BlockId;
const EdgeType = context_block.EdgeType;
const TypedArenaType = arena.TypedArenaType;
const ArenaOwnership = arena.ArenaOwnership;
const BlockOwnership = ownership.BlockOwnership;

pub const OwnedGraphEdge = struct {
    edge: GraphEdge,
    ownership: BlockOwnership,

    pub fn init(edge: GraphEdge, owner: BlockOwnership) OwnedGraphEdge {
        return OwnedGraphEdge{
            .edge = edge,
            .ownership = owner,
        };
    }

    pub fn read(self: *const OwnedGraphEdge, accessor: BlockOwnership) *const GraphEdge {
        if (accessor != self.ownership and accessor != .temporary) {
            assert.fatal_assert(false, "Edge access violation: {s} cannot read {s}-owned edge", .{ accessor.name(), self.ownership.name() });
        }
        return &self.edge;
    }

    /// Get underlying GraphEdge for query operations (returns by value, no pointer access)
    pub fn as_edge(self: *const OwnedGraphEdge) GraphEdge {
        return self.edge;
    }
};

pub const GraphEdgeIndex = struct {
    outgoing_edges: std.HashMap(
        BlockId,
        std.ArrayList(OwnedGraphEdge),
        BlockIdContext,
        std.hash_map.default_max_load_percentage,
    ),
    incoming_edges: std.HashMap(
        BlockId,
        std.ArrayList(OwnedGraphEdge),
        BlockIdContext,
        std.hash_map.default_max_load_percentage,
    ),
    edge_arena: TypedArenaType(GraphEdge, GraphEdgeIndex),
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
        return GraphEdgeIndex{
            .outgoing_edges = std.HashMap(
                BlockId,
                std.ArrayList(OwnedGraphEdge),
                BlockIdContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator),
            .incoming_edges = std.HashMap(
                BlockId,
                std.ArrayList(OwnedGraphEdge),
                BlockIdContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator),
            .edge_arena = TypedArenaType(GraphEdge, GraphEdgeIndex).init(allocator, .memtable_manager),
            .backing_allocator = allocator,
        };
    }

    pub fn deinit(self: *GraphEdgeIndex) void {
        var outgoing_iterator = self.outgoing_edges.valueIterator();
        while (outgoing_iterator.next()) |edge_list| {
            edge_list.deinit();
        }
        var incoming_iterator = self.incoming_edges.valueIterator();
        while (incoming_iterator.next()) |edge_list| {
            edge_list.deinit();
        }
        self.outgoing_edges.deinit();
        self.incoming_edges.deinit();

        self.edge_arena.deinit();
    }

    /// Add a directed edge to the index with bidirectional lookup support.
    /// Updates both outgoing and incoming edge collections for efficient traversal.
    /// Uses arena allocation for O(1) bulk cleanup during index reset operations.
    pub fn put_edge(self: *GraphEdgeIndex, edge: GraphEdge) !void {
        assert.assert_fmt(@intFromPtr(self) != 0, "GraphEdgeIndex self pointer cannot be null", .{});
        assert.assert_fmt(@intFromPtr(&self.edge_arena) != 0, "GraphEdgeIndex arena pointer cannot be null", .{});

        var source_non_zero: u32 = 0;
        var target_non_zero: u32 = 0;
        for (edge.source_id.bytes) |byte| {
            if (byte != 0) source_non_zero += 1;
        }
        for (edge.target_id.bytes) |byte| {
            if (byte != 0) target_non_zero += 1;
        }
        assert.assert_fmt(source_non_zero > 0, "Edge source_id cannot be all zeros", .{});
        assert.assert_fmt(target_non_zero > 0, "Edge target_id cannot be all zeros", .{});
        assert.assert_fmt(!std.mem.eql(u8, &edge.source_id.bytes, &edge.target_id.bytes), "Edge cannot be self-referential", .{});

        const owned_edge = OwnedGraphEdge.init(edge, .memtable_manager);

        var outgoing_result = try self.outgoing_edges.getOrPut(edge.source_id);
        if (!outgoing_result.found_existing) {
            outgoing_result.value_ptr.* = std.ArrayList(OwnedGraphEdge).init(self.backing_allocator);
        }
        const outgoing_before = outgoing_result.value_ptr.items.len;
        try outgoing_result.value_ptr.append(owned_edge); // tidy:ignore-perf dynamic edge collection size
        assert.assert_fmt(outgoing_result.value_ptr.items.len == outgoing_before + 1, "Outgoing edge append failed", .{});

        var incoming_result = try self.incoming_edges.getOrPut(edge.target_id);
        if (!incoming_result.found_existing) {
            incoming_result.value_ptr.* = std.ArrayList(OwnedGraphEdge).init(self.backing_allocator);
        }
        const incoming_before = incoming_result.value_ptr.items.len;
        try incoming_result.value_ptr.append(owned_edge); // tidy:ignore-perf dynamic edge collection size
        assert.assert_fmt(incoming_result.value_ptr.items.len == incoming_before + 1, "Incoming edge append failed", .{});
    }

    /// Find all outgoing edges from a source block with ownership validation.
    /// Returns owned edge collection that can be safely accessed by the specified accessor.
    /// Used for graph traversal operations that need ownership-validated edge access.
    pub fn find_outgoing_edges_with_ownership(
        self: *const GraphEdgeIndex,
        source_id: BlockId,
        accessor: BlockOwnership,
    ) ?[]const OwnedGraphEdge {
        assert.assert_fmt(@intFromPtr(self) != 0, "GraphEdgeIndex self pointer cannot be null", .{});

        var non_zero_bytes: u32 = 0;
        for (source_id.bytes) |byte| {
            if (byte != 0) non_zero_bytes += 1;
        }
        assert.assert_fmt(non_zero_bytes > 0, "Source block ID cannot be all zeros", .{});

        if (self.outgoing_edges.getPtr(source_id)) |owned_edge_list| {
            assert.assert_fmt(@intFromPtr(owned_edge_list.items.ptr) != 0 or owned_edge_list.items.len == 0, "Edge list has null pointer with non-zero length", .{});

            const owned_edges = owned_edge_list.items;
            if (owned_edges.len == 0) return &[_]OwnedGraphEdge{};

            _ = owned_edges[0].read(accessor);

            return owned_edges;
        }
        return null;
    }

    /// Find all incoming edges to a target block with ownership validation.
    /// Returns owned edge collection that can be safely accessed by the specified accessor.
    /// Used for reverse graph traversal operations that need ownership-validated edge access.
    pub fn find_incoming_edges_with_ownership(
        self: *const GraphEdgeIndex,
        target_id: BlockId,
        accessor: BlockOwnership,
    ) ?[]const OwnedGraphEdge {
        assert.assert_fmt(@intFromPtr(self) != 0, "GraphEdgeIndex self pointer cannot be null", .{});

        var non_zero_bytes: u32 = 0;
        for (target_id.bytes) |byte| {
            if (byte != 0) non_zero_bytes += 1;
        }
        assert.assert_fmt(non_zero_bytes > 0, "Target block ID cannot be all zeros", .{});

        if (self.incoming_edges.getPtr(target_id)) |owned_edge_list| {
            assert.assert_fmt(@intFromPtr(owned_edge_list.items.ptr) != 0 or owned_edge_list.items.len == 0, "Edge list has null pointer with non-zero length", .{});

            const owned_edges = owned_edge_list.items;
            if (owned_edges.len == 0) return &[_]OwnedGraphEdge{};

            _ = owned_edges[0].read(accessor);

            return owned_edges;
        }
        return null;
    }

    /// Remove all edges involving a specific block (when block is deleted).
    /// Cleans up both outgoing and incoming edge lists to maintain consistency.
    /// Note: This removes only direct edges; graph traversal cleanup for
    /// indirect references requires separate handling.
    /// Must deinitialize ArrayLists to prevent memory leaks.
    pub fn remove_block_edges(self: *GraphEdgeIndex, block_id: BlockId) void {
        // Deinitialize ArrayList before removing to prevent memory leak
        if (self.outgoing_edges.getPtr(block_id)) |edge_list| {
            edge_list.deinit();
        }
        if (self.incoming_edges.getPtr(block_id)) |edge_list| {
            edge_list.deinit();
        }

        _ = self.outgoing_edges.remove(block_id);
        _ = self.incoming_edges.remove(block_id);
    }

    /// Remove a specific edge between two blocks.
    /// Removes from both outgoing and incoming indexes to maintain consistency.
    /// Returns true if edge was found and removed, false if not found.
    pub fn remove_edge(self: *GraphEdgeIndex, source_id: BlockId, target_id: BlockId, edge_type: EdgeType) bool {
        var removed = false;

        if (self.outgoing_edges.getPtr(source_id)) |edge_list| {
            for (edge_list.items, 0..) |edge, i| {
                if (edge.target_id.eql(target_id) and edge.edge_type == edge_type) {
                    _ = edge_list.swapRemove(i);
                    removed = true;
                    break;
                }
            }
        }

        if (self.incoming_edges.getPtr(target_id)) |edge_list| {
            for (edge_list.items, 0..) |edge, i| {
                if (edge.source_id.eql(source_id) and edge.edge_type == edge_type) {
                    _ = edge_list.swapRemove(i);
                    break;
                }
            }
        }

        return removed;
    }

    /// Get total number of edges in the index.
    /// Counts outgoing edges only to avoid double-counting since each edge
    /// appears in both outgoing and incoming indexes.
    pub fn edge_count(self: *const GraphEdgeIndex) u32 {
        assert.assert_fmt(@intFromPtr(self) != 0, "GraphEdgeIndex self pointer cannot be null", .{});

        var total: u32 = 0;
        var iterator = self.outgoing_edges.iterator();
        while (iterator.next()) |entry| {
            const count = @as(u32, @intCast(entry.value_ptr.items.len));
            assert.assert_fmt(count < 1000000, "Suspicious edge count for single block: {}", .{count});
            total += count;
        }
        return total;
    }

    /// Get number of blocks that have outgoing edges.
    pub fn source_block_count(self: *const GraphEdgeIndex) u32 {
        return @intCast(self.outgoing_edges.count());
    }

    /// Get number of blocks that have incoming edges.
    pub fn target_block_count(self: *const GraphEdgeIndex) u32 {
        return @intCast(self.incoming_edges.count());
    }

    /// Clear all edges and reset arena for O(1) bulk deallocation.
    /// Retains HashMap capacity for efficient reuse after clearing.
    pub fn clear(self: *GraphEdgeIndex) void {
        // Deinit all ArrayLists before clearing to prevent memory leaks
        var outgoing_iterator = self.outgoing_edges.valueIterator();
        while (outgoing_iterator.next()) |edge_list| {
            edge_list.deinit();
        }
        var incoming_iterator = self.incoming_edges.valueIterator();
        while (incoming_iterator.next()) |edge_list| {
            edge_list.deinit();
        }

        self.outgoing_edges.clearRetainingCapacity();
        self.incoming_edges.clearRetainingCapacity();

        self.edge_arena.reset();
    }
};

const testing = std.testing;

test "graph edge index initialization creates empty index" {
    var index = GraphEdgeIndex.init(testing.allocator);
    defer index.deinit();

    try testing.expectEqual(@as(u32, 0), index.edge_count());
    try testing.expectEqual(@as(u32, 0), index.source_block_count());
    try testing.expectEqual(@as(u32, 0), index.target_block_count());
}

test "put and find edge operations work correctly" {
    var index = GraphEdgeIndex.init(testing.allocator);
    defer index.deinit();

    const source_id = BlockId.generate();
    const target_id = BlockId.generate();
    const edge = GraphEdge{
        .source_id = source_id,
        .target_id = target_id,
        .edge_type = EdgeType.calls,
    };

    try index.put_edge(edge);

    try testing.expectEqual(@as(u32, 1), index.edge_count());

    const outgoing = index.find_outgoing_edges(source_id);
    try testing.expect(outgoing != null);
    try testing.expectEqual(@as(usize, 1), outgoing.?.len);
    try testing.expect(outgoing.?[0].target_id.eql(target_id));
    try testing.expectEqual(EdgeType.calls, outgoing.?[0].edge_type);

    const incoming = index.find_incoming_edges(target_id);
    try testing.expect(incoming != null);
    try testing.expectEqual(@as(usize, 1), incoming.?.len);
    try testing.expect(incoming.?[0].source_id.eql(source_id));
    try testing.expectEqual(EdgeType.calls, incoming.?[0].edge_type);
}

test "multiple edges from same source are stored correctly" {
    var index = GraphEdgeIndex.init(testing.allocator);
    defer index.deinit();

    const source_id = BlockId.generate();
    const target1_id = BlockId.generate();
    const target2_id = BlockId.generate();

    const edge1 = GraphEdge{
        .source_id = source_id,
        .target_id = target1_id,
        .edge_type = EdgeType.calls,
    };

    const edge2 = GraphEdge{
        .source_id = source_id,
        .target_id = target2_id,
        .edge_type = EdgeType.imports,
    };

    try index.put_edge(edge1);
    try index.put_edge(edge2);

    try testing.expectEqual(@as(u32, 2), index.edge_count());

    const outgoing = index.find_outgoing_edges(source_id);
    try testing.expect(outgoing != null);
    try testing.expectEqual(@as(usize, 2), outgoing.?.len);
}

test "remove specific edge works correctly" {
    var index = GraphEdgeIndex.init(testing.allocator);
    defer index.deinit();

    const source_id = BlockId.generate();
    const target_id = BlockId.generate();

    const edge1 = GraphEdge{
        .source_id = source_id,
        .target_id = target_id,
        .edge_type = EdgeType.calls,
    };

    const edge2 = GraphEdge{
        .source_id = source_id,
        .target_id = target_id,
        .edge_type = EdgeType.imports,
    };

    try index.put_edge(edge1);
    try index.put_edge(edge2);

    try testing.expectEqual(@as(u32, 2), index.edge_count());

    const removed = index.remove_edge(source_id, target_id, EdgeType.calls);
    try testing.expect(removed);

    try testing.expectEqual(@as(u32, 1), index.edge_count());

    const outgoing = index.find_outgoing_edges(source_id);
    try testing.expect(outgoing != null);
    try testing.expectEqual(@as(usize, 1), outgoing.?.len);
    try testing.expectEqual(EdgeType.imports, outgoing.?[0].edge_type);
}

test "remove block edges cleans up all references" {
    var index = GraphEdgeIndex.init(testing.allocator);
    defer index.deinit();

    const block_a = BlockId.generate();
    const block_b = BlockId.generate();
    const block_c = BlockId.generate();

    try index.put_edge(GraphEdge{ .source_id = block_a, .target_id = block_b, .edge_type = EdgeType.calls });
    try index.put_edge(GraphEdge{ .source_id = block_b, .target_id = block_c, .edge_type = EdgeType.calls });
    try index.put_edge(GraphEdge{ .source_id = block_c, .target_id = block_a, .edge_type = EdgeType.calls });

    try testing.expectEqual(@as(u32, 3), index.edge_count());

    index.remove_block_edges(block_b);

    try testing.expectEqual(@as(u32, 1), index.edge_count());

    try testing.expect(index.find_outgoing_edges(block_a) == null);
    try testing.expect(index.find_outgoing_edges(block_b) == null);
    try testing.expect(index.find_outgoing_edges(block_c) != null);
}

test "clear operation resets index to empty state" {
    var index = GraphEdgeIndex.init(testing.allocator);
    defer index.deinit();

    for (0..5) |i| {
        const source_id = BlockId.generate();
        const target_id = BlockId.generate();
        try index.put_edge(GraphEdge{
            .source_id = source_id,
            .target_id = target_id,
            .edge_type = if (i % 2 == 0) EdgeType.calls else EdgeType.imports,
        });
    }

    try testing.expectEqual(@as(u32, 5), index.edge_count());

    index.clear();

    try testing.expectEqual(@as(u32, 0), index.edge_count());
    try testing.expectEqual(@as(u32, 0), index.source_block_count());
    try testing.expectEqual(@as(u32, 0), index.target_block_count());
}

test "bidirectional index consistency" {
    var index = GraphEdgeIndex.init(testing.allocator);
    defer index.deinit();

    const source_id = BlockId.generate();
    const target_id = BlockId.generate();
    const edge = GraphEdge{
        .source_id = source_id,
        .target_id = target_id,
        .edge_type = EdgeType.calls,
    };

    try index.put_edge(edge);

    const outgoing = index.find_outgoing_edges(source_id);
    const incoming = index.find_incoming_edges(target_id);

    try testing.expect(outgoing != null);
    try testing.expect(incoming != null);

    try testing.expect(outgoing.?[0].source_id.eql(incoming.?[0].source_id));
    try testing.expect(outgoing.?[0].target_id.eql(incoming.?[0].target_id));
    try testing.expectEqual(outgoing.?[0].edge_type, incoming.?[0].edge_type);
}

test "hash context provides good distribution for block ids" {
    const ctx = GraphEdgeIndex.BlockIdContext{};

    const id1 = BlockId.generate();
    const id2 = BlockId.generate();

    const hash1 = ctx.hash(id1);
    const hash2 = ctx.hash(id2);

    try testing.expect(hash1 != hash2);

    try testing.expectEqual(hash1, ctx.hash(id1));

    try testing.expect(ctx.eql(id1, id1));
    try testing.expect(!ctx.eql(id1, id2));
}

test "edge count accuracy with complex graph" {
    var index = GraphEdgeIndex.init(testing.allocator);
    defer index.deinit();

    const blocks = [_]BlockId{
        BlockId.generate(),
        BlockId.generate(),
        BlockId.generate(),
        BlockId.generate(),
    };

    const edges = [_]GraphEdge{
        GraphEdge{ .source_id = blocks[0], .target_id = blocks[1], .edge_type = EdgeType.calls },
        GraphEdge{ .source_id = blocks[0], .target_id = blocks[2], .edge_type = EdgeType.imports },
        GraphEdge{ .source_id = blocks[1], .target_id = blocks[2], .edge_type = EdgeType.calls },
        GraphEdge{ .source_id = blocks[1], .target_id = blocks[3], .edge_type = EdgeType.calls },
        GraphEdge{ .source_id = blocks[2], .target_id = blocks[3], .edge_type = EdgeType.imports },
    };

    for (edges) |edge| {
        try index.put_edge(edge);
    }

    try testing.expectEqual(@as(u32, 5), index.edge_count());
    try testing.expect(index.source_block_count() <= 4);
    try testing.expect(index.target_block_count() <= 4);
}
