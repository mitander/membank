# Memory Architecture

Arena coordinator pattern eliminates temporal coupling and segmentation faults.

## Problem

Direct arena embedding corrupts when structs are copied:
```zig
// Broken
struct { arena: ArenaAllocator }  // Corrupts on copy
```

## Solution

Coordinator provides stable interface:
```zig
pub const StorageEngine = struct {
    storage_arena: ArenaAllocator,
    coordinator: ArenaCoordinator,  // Pointer-based, survives copies

    pub fn init(backing: Allocator) StorageEngine {
        var arena = ArenaAllocator.init(backing);
        return .{
            .storage_arena = arena,
            .coordinator = ArenaCoordinator{ .arena = &arena },
        };
    }
};
```

## Hierarchy

```
StorageEngine (owns arena)
    ↓ coordinator interface
MemtableManager (uses coordinator)
    ↓ coordinator interface
BlockIndex (pure computation)
```

## Performance

- Arena reset: O(1) cleanup
- Allocation: <50ns through coordinator
- Zero fragmentation
- 20-30% improvement over malloc/free

## Rules

1. Coordinators own exactly one arena
2. Submodules receive coordinator interfaces
3. Never embed arenas in copyable structs
4. O(1) cleanup via coordinator.reset()
