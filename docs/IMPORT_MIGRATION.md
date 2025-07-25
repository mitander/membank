# Import Migration Plan: Module Dependencies → Relative Imports

This document outlines the migration from CortexDB's current module-based import system to TigerBeetle-style relative imports. This change will reduce build system complexity from 586 lines to ~200 lines while improving maintainability.

## Current State Analysis

### Build System Complexity
- `build.zig`: 586 lines managing explicit module dependencies
- `CoreModules` struct: 22 explicit imports with complex dependency graph
- Each module requires manual `addImport()` calls
- Circular dependency detection is manual and error-prone

### Current Import Pattern
```zig
// In build.zig
const vfs_module = b.createModule(.{
    .root_source_file = b.path("src/core/vfs.zig"),
});
vfs_module.addImport("assert", assert_module);

const storage_module = b.createModule(.{
    .root_source_file = b.path("src/storage/storage.zig"),
});
storage_module.addImport("vfs", vfs_module);
storage_module.addImport("context_block", context_block_module);
// ... 15 more imports
```

```zig
// In source files
const vfs = @import("vfs");
const context_block = @import("context_block");
```

## Target State: TigerBeetle Style

### Simplified Build System
```zig
// Simplified build.zig (~200 lines)
const cortexdb_exe = b.addExecutable(.{
    .name = "cortexdb",
    .root_source_file = b.path("src/main.zig"),
    .target = target,
    .optimize = optimize,
});
```

### Relative Import Pattern
```zig
// In source files
const vfs = @import("../core/vfs.zig");
const ContextBlock = @import("../core/context_block.zig").ContextBlock;
```

## Migration Strategy

### Dependency Analysis
Current module dependency graph (simplified):
```
assert (foundation)
├── stdx
├── vfs
├── context_block
└── concurrency

vfs (I/O abstraction)
├── production_vfs
└── simulation_vfs

storage (data layer)
├── wal
├── sstable
├── tiered_compaction
└── depends on: vfs, context_block, concurrency

query_engine (query layer)
└── depends on: storage, context_block

main (application)
└── depends on: storage, query_engine, ingestion
```

### Phase-by-Phase Conversion

#### Phase 1: Foundation Modules (Week 1)
Convert modules with no/minimal dependencies:

**Target modules:**
- `src/core/assert.zig` ✓ (no dependencies)
- `src/core/stdx.zig` ← assert
- `src/core/vfs.zig` ← assert
- `src/core/context_block.zig` ← assert, stdx
- `src/core/concurrency.zig` ← assert

**Conversion steps:**
1. Update import statements in each module
2. Remove from `CoreModules` struct
3. Update dependent modules one by one
4. Test compilation after each change

#### Phase 2: I/O Layer (Week 1-2)
**Target modules:**
- `src/core/production_vfs.zig` ← vfs, assert
- `src/core/simulation_vfs.zig` ← vfs, assert
- `src/core/simulation.zig` ← simulation_vfs, vfs

#### Phase 3: Storage Layer (Week 2-3)
**Target modules:**
- `src/storage/sstable.zig` ← vfs, context_block, assert
- `src/storage/wal_entry_stream.zig` ← vfs, assert
- `src/storage/wal.zig` ← vfs, context_block, wal_entry_stream
- `src/storage/tiered_compaction.zig` ← sstable, vfs
- `src/storage/storage.zig` ← wal, sstable, context_block, vfs

#### Phase 4: Query & Application Layer (Week 3-4)
**Target modules:**
- `src/query/query_engine.zig` ← storage, context_block
- `src/ingestion/*.zig` ← storage, context_block
- `src/main.zig` ← storage, query_engine, ingestion

#### Phase 5: Cleanup (Week 4)
- Remove `CoreModules` struct entirely
- Simplify build targets
- Update documentation

## Detailed Conversion Examples

### Before: Storage Module
```zig
// In build.zig
const storage_module = b.createModule(.{
    .root_source_file = b.path("src/storage/storage.zig"),
});
storage_module.addImport("assert", assert_module);
storage_module.addImport("vfs", vfs_module);
storage_module.addImport("context_block", context_block_module);
storage_module.addImport("concurrency", concurrency_module);
storage_module.addImport("wal", wal_module);
storage_module.addImport("sstable", sstable_module);

// In src/storage/storage.zig
const assert = @import("assert");
const vfs = @import("vfs");
const context_block = @import("context_block");
const concurrency = @import("concurrency");
const wal = @import("wal");
const sstable = @import("sstable");
```

### After: Storage Module
```zig
// build.zig - no explicit module management needed

// In src/storage/storage.zig
const assert = @import("../core/assert.zig");
const vfs = @import("../core/vfs.zig");
const context_block = @import("../core/context_block.zig");
const concurrency = @import("../core/concurrency.zig");
const wal = @import("wal.zig");
const sstable = @import("sstable.zig");
```

## File-by-File Conversion Checklist

### Foundation Layer
- [ ] `src/core/stdx.zig` - Update assert import
- [ ] `src/core/vfs.zig` - Update assert import
- [ ] `src/core/context_block.zig` - Update assert, stdx imports
- [ ] `src/core/concurrency.zig` - Update assert import
- [ ] `src/core/error_context.zig` - Update assert import

### I/O Layer
- [ ] `src/core/production_vfs.zig` - Update vfs, assert imports
- [ ] `src/core/simulation_vfs.zig` - Update vfs, assert imports
- [ ] `src/core/simulation.zig` - Update simulation_vfs, vfs imports

### Storage Layer
- [ ] `src/storage/sstable.zig` - Update all imports to relative
- [ ] `src/storage/wal_entry_stream.zig` - Update vfs, assert imports
- [ ] `src/storage/wal.zig` - Update to relative imports
- [ ] `src/storage/tiered_compaction.zig` - Update sstable, vfs imports
- [ ] `src/storage/storage.zig` - Update all storage layer imports

### Query Layer
- [ ] `src/query/query_engine.zig` - Update storage, context_block imports

### Application Layer
- [ ] `src/ingestion/*.zig` - Update all imports
- [ ] `src/main.zig` - Update all imports

### Test Files
- [ ] `tests/**/*.zig` - Update all imports systematically

## Risk Mitigation

### Compilation Validation
After each phase:
```bash
./zig/zig build test
./zig/zig build cortexdb
```

### Incremental Testing Strategy
1. Convert one module at a time
2. Ensure compilation succeeds after each change
3. Run focused tests: `./zig/zig build test --test-filter="module_name"`
4. Commit after each successful module conversion

### Rollback Plan
- Each phase gets its own feature branch
- Atomic commits for each module conversion
- Easy rollback if compilation breaks

### Dependency Tracking
Maintain dependency graph manually during conversion:
```
Phase 1: assert → stdx → vfs → context_block → concurrency
Phase 2: production_vfs, simulation_vfs ← vfs
Phase 3: sstable ← vfs,context_block; wal ← vfs,context_block,wal_entry_stream
Phase 4: storage ← all storage modules; query_engine ← storage
```

## Expected Benefits

### Build System Simplification
- **Before**: 586 lines, 22 explicit modules, complex dependency management
- **After**: ~200 lines, simple compilation units, automatic dependency resolution

### Developer Experience
- Faster incremental compilation
- Clearer import relationships
- Easier to add new modules
- No manual dependency graph maintenance

### Maintainability
- Reduced cognitive load for new contributors
- Self-documenting file relationships
- Easier refactoring (file moves don't break build system)

## Validation Criteria

### Functional Requirements
- [ ] All tests pass after migration
- [ ] `./zig/zig build test` completes successfully
- [ ] `./zig/zig build cortexdb` produces working executable
- [ ] No change in runtime behavior

### Performance Requirements
- [ ] Clean build time < 2 minutes (target from current ~3 minutes)
- [ ] Incremental build time < 30 seconds
- [ ] Memory usage during compilation < 2GB

### Code Quality Requirements
- [ ] No circular imports introduced
- [ ] All imports use relative paths
- [ ] No unused imports remain
- [ ] Import statements follow consistent ordering

---

*Estimated Effort*: 3-4 weeks (1 week per phase)
*Risk Level*: Low (incremental, reversible changes)
*Dependencies*: None (can start immediately)
