# KausalDB Memory Architecture

## Overview

KausalDB employs a sophisticated hybrid memory model designed for microsecond-level performance and deterministic behavior. This document defines the authoritative memory management patterns and their correct usage throughout the codebase.

## Core Principles

1. **Explicit Lifetime Management**: Every allocation has a clearly defined owner and lifetime
2. **Zero-Cost Abstractions**: Memory safety mechanisms compile to zero overhead in release builds
3. **Arena-First Design**: Bulk allocation/deallocation for predictable performance
4. **Data-Oriented Layout**: Structure-of-Arrays for cache-optimal access patterns

## Memory Hierarchy

### Level 1: Permanent Infrastructure (GeneralPurposeAllocator)

Long-lived components that exist for the entire program lifetime.

**Lifetime**: Program initialization → Program termination
**Examples**: `StorageEngine`, `SSTableManager`, `QueryEngine` root structs
**Pattern**:
```zig
// Allocated once at startup
const engine = try allocator.create(StorageEngine);
defer allocator.destroy(engine);
```

### Level 2: Subsystem Arenas (ArenaAllocator + Coordinator)

Memory pools owned by major subsystems, reset periodically.

**Lifetime**: Subsystem startup → Explicit reset/flush
**Examples**: `BlockIndex` memory, SSTable cache, WAL buffers
**Pattern**:
```zig
// Arena coordinator provides stable allocation interface
pub const BlockIndex = struct {
    arena_coordinator: *const ArenaCoordinator,
    // ... other fields

    pub fn put_block(self: *BlockIndex, block: ContextBlock) !void {
        // Always allocate through coordinator for stability
        const content = try self.arena_coordinator.duplicate_slice(u8, block.content);
        // ...
    }
};
```

### Level 3: Task Arenas (ArenaAllocator)

Temporary memory for bounded operations.

**Lifetime**: Operation start → Operation complete
**Examples**: Query execution, compaction, WAL recovery
**Pattern**:
```zig
// Create arena for task duration
var task_arena = std.heap.ArenaAllocator.init(backing_allocator);
defer task_arena.deinit();

// All task allocations use this arena
const result = try process_query(task_arena.allocator(), query);
// Arena cleanup frees everything at once
```

### Level 4: Stack Allocation

Truly ephemeral data with function-scoped lifetime.

**Lifetime**: Function entry → Function exit
**Examples**: Temporary buffers, small arrays
**Critical Rule**: NEVER return pointers to stack data
**Pattern**:
```zig
// GOOD: Stack buffer for immediate use
var buffer: [4096]u8 = undefined;
const bytes_read = try file.read(&buffer);
process_bytes(buffer[0..bytes_read]);

// BAD: Returning stack pointer
fn bad_pattern() *const Data {
    var data = Data{};  // Stack allocated
    return &data;       // DANGLING POINTER!
}
```

## The Arena Coordinator Pattern

The Arena Coordinator solves the critical problem of arena lifetime management by providing a stable interface that survives arena resets.

### Problem It Solves

```zig
// PROBLEM: Direct arena embedding
pub const BadPattern = struct {
    arena: std.heap.ArenaAllocator,  // Embedded arena

    fn bad_method(self: *BadPattern) !void {
        // If 'self' is copied, arena state corrupts!
        const data = try self.arena.allocator().alloc(u8, 100);
    }
};
```

### Solution: Coordinator Indirection

```zig
// SOLUTION: Stable coordinator interface
pub const GoodPattern = struct {
    arena_coordinator: *const ArenaCoordinator,  // Pointer to coordinator

    fn good_method(self: *GoodPattern) !void {
        // Coordinator remains valid even if struct is moved
        const data = try self.arena_coordinator.alloc(u8, 100);
    }
};
```

## Ownership Model

### Compile-Time Ownership (Zero-Cost)

For performance-critical paths where ownership is known at compile time:

```zig
pub const StorageEngineBlock = struct {
    block: ContextBlock,

    pub fn init(block: ContextBlock) StorageEngineBlock {
        return .{ .block = block };
    }
};

// Usage: Zero runtime overhead
fn find_storage_block(self: *StorageEngine, id: BlockId) !?StorageEngineBlock {
    // Direct access, no ownership checks
    return StorageEngineBlock.init(found_block);
}
```

### Runtime Ownership (Validated)

For dynamic ownership scenarios:

```zig
pub const OwnedBlock = struct {
    block: ContextBlock,
    ownership: BlockOwnership,
    state: enum { valid, moved },

    pub fn transfer(self: *OwnedBlock, new_owner: BlockOwnership) OwnedBlock {
        assert(self.state == .valid);
        self.state = .moved;  // Invalidate source
        return .{ .block = self.block, .ownership = new_owner, .state = .valid };
    }
};
```

## Data-Oriented Design Guidelines

### Current Anti-Pattern (Array-of-Structs)
```zig
// Poor cache locality - each access may cause cache miss
blocks: HashMap(BlockId, *ContextBlock),
```

### Recommended Pattern (Struct-of-Arrays)
```zig
pub const BlockIndexDOD = struct {
    // Contiguous arrays for cache-friendly iteration
    ids: ArrayList(BlockId),
    versions: ArrayList(u64),
    contents: ArrayList([]const u8),
    metadata: ArrayList([]const u8),

    // Index maps ID to array position
    lookup: HashMap(BlockId, u32),

    pub fn find_by_version(self: *BlockIndexDOD, min_version: u64) []BlockId {
        // Scanning versions array is cache-optimal
        var results = ArrayList(BlockId).init(self.allocator);
        for (self.versions.items, 0..) |version, i| {
            if (version >= min_version) {
                results.append(self.ids.items[i]) catch continue;
            }
        }
        return results.toOwnedSlice();
    }
};
```

## Common Patterns and Anti-Patterns

### Pattern: Heap-Allocated Iterators
```zig
// GOOD: Heap allocation ensures stable addresses
pub fn create_iterator(self: *Manager) !*Iterator {
    const iter = try self.allocator.create(Iterator);
    iter.* = Iterator.init(self);
    return iter;
}
```

### Anti-Pattern: Stack-Allocated Long-Lived Objects
```zig
// BAD: Stack object with references
pub fn bad_iterator(self: *Manager) Iterator {
    var iter = Iterator.init(self);  // Stack allocated
    return iter;  // If iter holds pointers to self, they become invalid
}
```

### Pattern: Arena-Safe Deserialization
```zig
// GOOD: Buffer allocated from same arena as result
pub fn deserialize(arena: *ArenaCoordinator, data: []const u8) !Block {
    const buffer = try arena.duplicate_slice(u8, data);
    return Block{
        .content = buffer,
        // ...
    };
}
```

### Anti-Pattern: Mixed Allocator Usage
```zig
// BAD: Temporary allocator for persistent data
pub fn bad_deserialize(backing: Allocator, data: []const u8) !Block {
    const buffer = try backing.alloc(u8, data.len);
    defer backing.free(buffer);  // Buffer freed before Block uses it!
    @memcpy(buffer, data);
    return Block{ .content = buffer };
}
```

## Performance Optimization Strategies

### 1. Pool Allocation for Fixed-Size Objects

```zig
pub const SSTTablePool = struct {
    pool: std.heap.MemoryPool(SSTable),

    pub fn acquire(self: *SSTTablePool) !*SSTable {
        return self.pool.create();
    }

    pub fn release(self: *SSTTablePool, table: *SSTable) void {
        table.deinit();
        self.pool.destroy(table);
    }
};
```

### 2. Zero-Copy Read Path

```zig
pub const ZeroCopyIterator = struct {
    source: *const BlockIndex,
    position: usize,

    pub fn next(self: *ZeroCopyIterator) ?*const ContextBlock {
        // Return direct pointer to source data
        // Lifetime tied to source BlockIndex
        if (self.position >= self.source.count()) return null;
        const block = self.source.get_by_index(self.position);
        self.position += 1;
        return block;
    }
};
```

### 3. Batch Operations

```zig
pub fn put_blocks_batch(self: *BlockIndex, blocks: []const ContextBlock) !void {
    // Pre-calculate total memory needed
    var total_size: usize = 0;
    for (blocks) |block| {
        total_size += block.content.len + block.metadata_json.len;
    }

    // Single allocation for all blocks
    const batch_buffer = try self.arena_coordinator.alloc(u8, total_size);
    var offset: usize = 0;

    for (blocks) |block| {
        const content_slice = batch_buffer[offset..offset + block.content.len];
        @memcpy(content_slice, block.content);
        // ... store block with slice reference
        offset += block.content.len;
    }
}
```

## Memory Safety Checklist

Before committing code, verify:

- [ ] **No Stack Pointers Escape**: Functions never return pointers to stack variables
- [ ] **Arena Lifetime Matches Data**: Data lifetime doesn't exceed arena lifetime
- [ ] **Coordinator Pattern for Subsystems**: Long-lived components use ArenaCoordinator
- [ ] **Ownership Transfers are Explicit**: Use `transfer()` method with state tracking
- [ ] **Consistent Allocator Usage**: Don't mix allocators for related data
- [ ] **Heap Allocation for Iterators**: Iterators with state use heap allocation
- [ ] **DOD for Hot Paths**: Performance-critical indexes use Struct-of-Arrays

## Migration Path

### Phase 1: Fix Critical Bugs (COMPLETED)
- [x] Fix SSTableIterator lifetime issue
- [x] Implement heap allocation for BlockIterator's SSTable

### Phase 2: Standardize Patterns (IN PROGRESS)
- [ ] Migrate all subsystems to ArenaCoordinator pattern
- [ ] Add ownership state tracking to OwnedBlock
- [ ] Remove dangerous `transfer_ownership` footgun

### Phase 3: Performance Optimization (PLANNED)
- [ ] Convert BlockIndex to Struct-of-Arrays
- [ ] Implement zero-copy query path
- [ ] Add fixed-size object pools

### Phase 4: Validation (PLANNED)
- [ ] Add arena lifetime validation in debug builds
- [ ] Create ownership violation tests
- [ ] Benchmark memory patterns

## Benchmarking Memory Patterns

```zig
// Measure allocation overhead
const start = std.time.nanoTimestamp();
for (0..iterations) |_| {
    const block = try arena.create(Block);
    // ...
}
const elapsed = std.time.nanoTimestamp() - start;
const ns_per_alloc = elapsed / iterations;

// Target: < 10ns per allocation for hot paths
```

## Summary

KausalDB's memory model is designed for deterministic, high-performance operation:

1. **Use the right allocator for the job**: GPA for infrastructure, arenas for tasks
2. **Follow the Arena Coordinator pattern**: Stable interfaces for subsystem memory
3. **Embrace Data-Oriented Design**: Struct-of-Arrays for cache performance
4. **Make ownership explicit**: Compile-time when possible, validated runtime when necessary
5. **Never let stack pointers escape**: Heap-allocate iterators and long-lived objects

This architecture provides the foundation for KausalDB's microsecond-level performance targets while maintaining memory safety and deterministic behavior.
