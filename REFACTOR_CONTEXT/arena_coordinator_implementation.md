# Arena Coordinator Pattern Implementation Guide

## Executive Summary

**Problem**: Arena allocators embedded in structs cause segmentation faults when structs are copied via assignment (`ptr.* = try SomeStruct.init()`). Arena's internal SinglyLinkedList contains self-referential pointers that become invalid when copied.

**Solution**: Replace embedded arenas with Arena Coordinator interfaces that provide stable allocation access regardless of underlying arena operations.

**Goal**: Eliminate segfaults while maintaining simple APIs where components only need `.deinit()` for cleanup.

## Problem Analysis

### Root Cause
Arena allocators contain internal `buffer_list: SinglyLinkedList(BufNode)` with self-referential pointers. When a struct containing an arena is copied:
```zig
var storage_engine = try StorageEngine.init_default(allocator, vfs, "data");
// Arena's buffer_list pointers now point to the temporary struct from init_default()
```
The arena's internal pointers reference the original (now deallocated) struct location, causing segfaults on any allocation.

### Current Failures
- Multiple segfaults in test suite: `wal_streaming_recovery`, `enhanced_compaction_strategies`, `ingestion_faults`, etc.
- API instability from repeated `create_default()` / heap allocation workarounds
- Complex cleanup patterns requiring `allocator.destroy()` calls

### Impact
- Blocks v0.1.0 release
- Creates developer friction with unstable APIs
- Masks real bugs with workaround complexity

## Solution: Arena Coordinator Pattern

### Design Principles
1. **Stable Interfaces**: Coordinator interfaces never become invalid
2. **Simple APIs**: Components only need `.deinit()` for cleanup
3. **Zero Temporal Coupling**: Arena operations don't invalidate component access
4. **Performance Maintained**: <20μs write latency preserved

### Core Pattern
```zig
pub const ArenaCoordinator = struct {
    arena: *std.heap.ArenaAllocator,

    pub fn allocator(self: *const ArenaCoordinator) std.mem.Allocator {
        return self.arena.allocator();
    }

    pub fn duplicate_slice(self: *const ArenaCoordinator, comptime T: type, slice: []const T) ![]T {
        return self.arena.allocator().dupe(T, slice);
    }

    pub fn reset(self: *ArenaCoordinator) void {
        self.arena.reset(.retain_capacity);
    }
};
```

## Implementation Plan

### Phase 1: Core Infrastructure (Day 1)

**1.1 Create ArenaCoordinator**
- File: `src/core/memory.zig`
- Implement `ArenaCoordinator` struct with allocation methods
- Add debug validation methods
- Create unit tests

**1.2 Update StorageEngine**
- File: `src/storage/engine.zig`
- Keep `init_default()` API but fix arena corruption
- Add `arena_coordinator` field pointing to internal arena
- Ensure `deinit()` only needs to call `storage_arena.deinit()`

**1.3 Validate Basic Functionality**
- Run `./zig/zig build test` to ensure no regressions
- Verify `tests/storage/engine_test.zig` passes

### Phase 2: Storage Subsystem (Day 2)

**2.1 Update MemtableManager**
- File: `src/storage/memtable_manager.zig`
- Replace `arena_allocator: std.mem.Allocator` with `arena_coordinator: ArenaCoordinator`
- Update all allocation calls to use coordinator methods
- Fix `init()` signature to accept coordinator

**2.2 Update BlockIndex**
- File: `src/storage/block_index.zig`
- Replace direct arena usage with coordinator interface
- Update `put_block()` to use `coordinator.duplicate_slice()`
- Fix storage engine reference pattern

**2.3 Update GraphEdgeIndex**
- File: `src/storage/graph_edge_index.zig`
- Similar pattern to BlockIndex
- Replace arena references with coordinator interface

**2.4 Validate Storage Tests**
- Run storage-specific tests: `./zig/zig build test-storage`
- Fix any compilation errors from signature changes

### Phase 3: Additional Components (Day 3)

**3.1 Update SSTableManager**
- File: `src/storage/sstable_manager.zig`
- Replace arena usage with coordinator pattern
- Maintain existing path ownership model

**3.2 Update SimulationVFS**
- File: `src/sim/simulation_vfs.zig`
- Apply arena coordinator pattern if it uses embedded arenas
- Focus on `heap_init()` methods that were added as workarounds

**3.3 Update QueryEngine**
- File: `src/query/engine.zig`
- Apply coordinator pattern for query result arenas
- Maintain existing query result iterator performance

### Phase 4: Test Suite Fixes (Day 4)

**4.1 Revert API Changes**
- Remove all `create_default()` methods
- Revert tests back to `init_default()` usage
- Remove complex `allocator.destroy()` cleanup patterns

**4.2 Fix Specific Failing Tests**
- `tests/recovery/wal_streaming_recovery.zig`
- `tests/storage/enhanced_compaction_strategies.zig`
- `tests/fault_injection/ingestion_faults.zig`
- `tests/fault_injection/server_faults.zig`

**4.3 Fix Benchmark Code**
- `src/dev/benchmark/storage.zig`
- `src/dev/benchmark/query.zig`
- `src/dev/benchmark/compaction.zig`
- Apply coordinator pattern consistently

### Phase 5: Validation & Documentation (Day 5)

**5.1 Comprehensive Testing**
- Run full test suite: `./zig/zig build test-all`
- Validate zero segmentation faults
- Check memory leak detection

**5.2 Performance Validation**
- Run benchmarks: `./zig/zig build benchmark`
- Ensure <20μs write latency maintained
- Validate memory usage patterns

**5.3 Update Documentation**
- Already updated `docs/DESIGN.md` and `docs/STYLE.md`
- Add memory model examples to `docs/DEVELOPMENT.md`

## Code Patterns

### Before (Broken)
```zig
pub const StorageEngine = struct {
    storage_arena: std.heap.ArenaAllocator,  // Embedded arena - CORRUPTED on copy

    pub fn init_default(allocator: std.mem.Allocator, vfs: VFS, data_dir: []const u8) !StorageEngine {
        return StorageEngine{
            .storage_arena = std.heap.ArenaAllocator.init(allocator),
            // When this struct is copied, arena pointers become invalid
        };
    }
};
```

### After (Fixed)
```zig
pub const StorageEngine = struct {
    storage_arena: std.heap.ArenaAllocator,     // Owned arena
    arena_coordinator: ArenaCoordinator,        // Stable interface

    pub fn init_default(allocator: std.mem.Allocator, vfs: VFS, data_dir: []const u8) !StorageEngine {
        var storage_arena = std.heap.ArenaAllocator.init(allocator);
        return StorageEngine{
            .storage_arena = storage_arena,
            .arena_coordinator = ArenaCoordinator{ .arena = &storage_arena },
            // Coordinator interface remains valid even if struct is copied
        };
    }

    pub fn deinit(self: *StorageEngine) void {
        self.storage_arena.deinit(); // Simple cleanup
    }
};
```

### Submodule Pattern
```zig
pub const MemtableManager = struct {
    arena_coordinator: ArenaCoordinator,        // Never invalid
    backing_allocator: std.mem.Allocator,

    pub fn init(coordinator: ArenaCoordinator, backing: std.mem.Allocator) MemtableManager {
        return MemtableManager{
            .arena_coordinator = coordinator,
            .backing_allocator = backing,
        };
    }

    pub fn allocate_block_content(self: *MemtableManager, content: []const u8) ![]u8 {
        return self.arena_coordinator.duplicate_slice(u8, content);
    }
};
```

## Testing Strategy

### Unit Tests
- Test `ArenaCoordinator` allocation methods
- Test coordinator interface remains valid after arena reset
- Test zero-copy allocation patterns

### Integration Tests
- Verify storage engine initialization without segfaults
- Test memtable operations with coordinator allocation
- Validate cleanup simplicity (only `.deinit()` needed)

### Memory Safety Tests
- Run with `std.heap.GeneralPurposeAllocator(.{ .safety = true })`
- Validate zero memory leaks
- Test fault injection doesn't cause arena corruption

### Performance Tests
- Benchmark allocation overhead of coordinator pattern
- Ensure <20μs write latency maintained
- Validate memory usage characteristics

## Risk Mitigation

### Compilation Errors
- Update method signatures incrementally
- Fix import statements for new `ArenaCoordinator` type
- Test compilation after each component update

### Performance Regression
- Benchmark before/after each phase
- Monitor allocation patterns in hot paths
- Inline coordinator methods if needed

### API Compatibility
- Maintain existing `init_default()` signatures
- Preserve existing `deinit()` patterns
- Avoid breaking changes to public APIs

### Memory Safety
- Add debug assertions for coordinator validity
- Test with memory safety tools
- Validate arena ownership patterns

## Success Criteria

### Functional
- [ ] Zero segmentation faults in full test suite
- [ ] All components cleanup with single `.deinit()` call
- [ ] No `create_default()` or `allocator.destroy()` patterns needed

### Performance
- [ ] Write latency <20μs maintained
- [ ] Memory usage patterns unchanged
- [ ] Zero additional allocations in hot paths

### Quality
- [ ] Simple, stable APIs throughout codebase
- [ ] Clear memory ownership hierarchy
- [ ] Comprehensive test coverage for memory patterns

## Key Files to Modify

### Core Infrastructure
- `src/core/memory.zig` (new file)
- `src/storage/engine.zig`

### Storage Components
- `src/storage/memtable_manager.zig`
- `src/storage/block_index.zig`
- `src/storage/graph_edge_index.zig`
- `src/storage/sstable_manager.zig`

### Tests (revert to simple patterns)
- `tests/recovery/wal_streaming_recovery.zig`
- `tests/storage/enhanced_compaction_strategies.zig`
- `tests/fault_injection/ingestion_faults.zig`
- `tests/fault_injection/server_faults.zig`

### Benchmarks
- `src/dev/benchmark/storage.zig`
- `src/dev/benchmark/query.zig`
- `src/dev/benchmark/compaction.zig`

## Implementation Notes

1. **Start Small**: Begin with `ArenaCoordinator` and `StorageEngine` only
2. **Test Frequently**: Run tests after each component update
3. **Maintain Performance**: Benchmark allocation-heavy operations
4. **Preserve APIs**: Keep existing method signatures where possible
5. **Validate Early**: Check for segfaults immediately after each change

This refactor eliminates the arena corruption crisis while maintaining the performance benefits and simplifying the memory management model throughout the codebase.
