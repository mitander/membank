# Memory Model Improvements for KausalDB

## Executive Summary

This document tracks critical improvements to KausalDB's memory model, addressing architectural violations and performance bottlenecks discovered during the recent debugging session. The improvements focus on eliminating entire classes of bugs through compile-time safety and data-oriented design.

## Critical Issues Fixed

### 1. Arena Alignment Panic (FIXED)
**Problem**: SSTableIterator created stack-allocated SSTable instances, causing arena coordinator corruption when structs were moved.
**Solution**: Heap-allocate SSTable instances in BlockIterator to ensure stable memory addresses.
**Impact**: Eliminated segmentation faults and arena corruption panics.

### 2. Buffer Lifetime Violations (FIXED)
**Problem**: SSTableIterator used stack-allocated buffers with defer-free, causing use-after-free in deserialization.
**Solution**: Allocate buffers from arena coordinator, letting arena manage lifetime.
**Impact**: Prevented dangling pointers in block deserialization.

## High-Priority Improvements

### 1. Introduce "Moved-From" State for Ownership Transfers
**Problem**: `transfer_ownership` allows use-after-transfer bugs.
**Implementation**:
```zig
pub const OwnedBlock = struct {
    block: ContextBlock,
    ownership: BlockOwnership,
    state: enum { valid, moved }, // Add state tracking

    pub fn transfer(self: *OwnedBlock, new_ownership: BlockOwnership) OwnedBlock {
        assert.fatal_assert(self.state == .valid, "Use-after-transfer detected", .{});
        self.state = .moved; // Invalidate source
        return OwnedBlock{
            .block = self.block,
            .ownership = new_ownership,
            .state = .valid
        };
    }
};
```
**Benefit**: Compile-time + runtime safety against use-after-transfer.
**Effort**: 2 hours
**Risk**: Low - additive change

### 2. Replace `*anyopaque` with Typed Coordinator Interface
**Problem**: Type-unsafe circular dependency breaking.
**Implementation**:
```zig
pub const StorageCoordinator = struct {
    ptr: *StorageEngine,

    pub fn duplicate_storage(self: StorageCoordinator, comptime T: type, slice: []const T) ![]T {
        return self.ptr.storage_arena.allocator().dupe(T, slice);
    }
};
```
**Benefit**: Type safety, clearer dependency graph.
**Effort**: 4 hours
**Risk**: Medium - requires refactoring multiple subsystems

### 3. Data-Oriented BlockIndex Redesign
**Problem**: Pointer chasing causes cache misses on hot paths.
**Implementation**: Convert from HashMap of pointers to Struct-of-Arrays.
```zig
pub const BlockIndexDOD = struct {
    // Contiguous arrays for cache-friendly iteration
    ids: ArrayList(BlockId),
    versions: ArrayList(u64),
    source_uris: ArrayList([]const u8),
    contents: ArrayList([]const u8),

    // Index for O(1) lookup
    lookup: HashMap(BlockId, u32),
};
```
**Benefit**: 3-5x performance improvement for scan operations.
**Effort**: 8 hours
**Risk**: High - fundamental architecture change

### 4. Zero-Copy Query Path
**Problem**: Unnecessary block cloning during queries.
**Implementation**: Return direct pointers with lifetime guarantees.
```zig
pub fn find_block_zero_copy(self: *QueryEngine, id: BlockId) !?*const ContextBlock {
    // Return pointer directly from storage, no allocation
    if (self.storage.find_block_ptr(id)) |ptr| {
        return ptr; // Lifetime tied to storage engine
    }
    return null;
}
```
**Benefit**: Eliminates allocations on read path.
**Effort**: 6 hours
**Risk**: Medium - requires careful lifetime management

### 5. Fixed-Size Object Pools
**Problem**: Frequent allocation/deallocation of SSTable and Iterator objects.
**Implementation**: Pre-allocated pools for common objects.
```zig
pub const ObjectPools = struct {
    sstable_pool: std.heap.MemoryPool(SSTable),
    iterator_pool: std.heap.MemoryPool(BlockIterator),

    pub fn init(allocator: Allocator) ObjectPools {
        return .{
            .sstable_pool = std.heap.MemoryPool(SSTable).init(allocator),
            .iterator_pool = std.heap.MemoryPool(BlockIterator).init(allocator),
        };
    }
};
```
**Benefit**: Predictable allocation performance, zero fragmentation.
**Effort**: 4 hours
**Risk**: Low - additive optimization

## Implementation Roadmap

### Week 1: Safety Hardening
- [ ] Add moved-from state to OwnedBlock
- [ ] Remove individual `deinit` methods from arena-allocated types
- [ ] Add debug-mode arena lifetime validation

### Week 2: Type Safety
- [ ] Implement StorageCoordinator interface
- [ ] Migrate BlockIndex to typed coordinator
- [ ] Migrate MemtableManager to typed coordinator

### Week 3: Performance Critical
- [ ] Implement zero-copy query path
- [ ] Add fixed-size object pools
- [ ] Benchmark before/after

### Week 4: Data-Oriented Redesign
- [ ] Design new BlockIndexDOD structure
- [ ] Implement parallel to existing BlockIndex
- [ ] A/B test performance
- [ ] Migrate if 2x+ improvement

## Testing Strategy

### Correctness Tests
```zig
test "ownership transfer invalidates source" {
    var block = OwnedBlock.init(test_block, .storage_engine);
    const transferred = block.transfer(.query_engine);

    // This should panic in debug builds
    try testing.expectPanic(block.read(.storage_engine));
}
```

### Performance Benchmarks
```zig
test "zero-copy vs clone performance" {
    // Measure 1M block reads
    const iterations = 1_000_000;

    // Current approach with cloning
    const clone_start = std.time.nanoTimestamp();
    for (0..iterations) |_| {
        const block = try engine.find_block_with_clone(test_id);
        defer allocator.free(block);
    }
    const clone_time = std.time.nanoTimestamp() - clone_start;

    // Zero-copy approach
    const zero_copy_start = std.time.nanoTimestamp();
    for (0..iterations) |_| {
        const block_ptr = try engine.find_block_zero_copy(test_id);
        _ = block_ptr;
    }
    const zero_copy_time = std.time.nanoTimestamp() - zero_copy_start;

    // Expect at least 5x improvement
    try testing.expect(zero_copy_time < clone_time / 5);
}
```

### Simulation Tests
```zig
test "arena coordinator survives struct moves" {
    var arena = ArenaAllocator.init(allocator);
    defer arena.deinit();

    const coordinator = ArenaCoordinator.init(&arena);
    var index = BlockIndex.init(&coordinator, allocator);

    // Move the index (simulates passing by value)
    var moved_index = index;

    // Should still work through coordinator
    try moved_index.put_block(test_block);
}
```

## Success Metrics

1. **Zero arena-related panics** in 1000 simulation runs
2. **< 10ns per allocation** for pool-allocated objects
3. **< 100ns** for block lookups (currently ~500ns)
4. **Zero allocations** on query read path
5. **50% reduction** in memory fragmentation

## Long-Term Vision

The ultimate goal is a memory model that:
- **Never panics** due to lifetime issues (enforced at compile time where possible)
- **Never fragments** (arena + pool allocation only)
- **Never allocates** on hot paths (zero-copy + pre-allocation)
- **Never confuses** developers (clear ownership semantics)

This positions KausalDB as a best-in-class example of Zig systems programming, worthy of study alongside TigerBeetle and other high-performance databases.

## Notes for Implementation

- Always benchmark before/after changes
- Add comprehensive tests for each improvement
- Document patterns in code comments, not just docs
- Consider gradual migration for high-risk changes
- Keep old implementation available for rollback

## Review Checklist

Before implementing each improvement:
- [ ] Does it eliminate an entire class of bugs?
- [ ] Does it improve performance on hot paths?
- [ ] Does it make the code more obvious?
- [ ] Can it be tested deterministically?
- [ ] Is the migration path clear?
