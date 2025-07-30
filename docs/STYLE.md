# Membank Style Guide

## Principles

1. **Explicit is Better Than Implicit** - No hidden allocations, control flow, or state
2. **Simplicity is the Prerequisite for Reliability** - Complex code kills systems
3. **Correctness is Not Negotiable** - Use types and assertions to prevent bugs

## Memory Management

### Allocator Parameters

Functions that allocate must accept explicit allocators:

```zig
// Good
pub fn init(allocator: std.mem.Allocator) !MyStruct { ... }

// Bad
pub fn init() !MyStruct { ... }
```

### Arena-per-Subsystem Pattern

Complex state uses dedicated arenas for bulk deallocation:

```zig
pub const MemtableManager = struct {
    arena: std.heap.ArenaAllocator,
    blocks: HashMap(BlockId, ContextBlock, ...),

    pub fn clear(self: *MemtableManager) void {
        self.blocks.clearRetainingCapacity();
        _ = self.arena.reset(.retain_capacity);  // O(1) cleanup
    }
};
```

### Hot Path Rules

- No allocations during queries
- Use temporary arenas for short-lived data
- Pre-allocate buffers when possible

## Naming Conventions

| Type      | Convention             | Examples                    | Bad Examples           |
| --------- | ---------------------- | --------------------------- | ---------------------- |
| Types     | `PascalCase`           | `StorageEngine`, `BlockId`  | `storage_engine`       |
| Functions | `snake_case`           | `find_block`, `put_edge`    | `findBlock`, `putEdge` |
| Variables | `snake_case`           | `block_count`, `wal_file`   | `blockCount`           |
| Constants | `SCREAMING_SNAKE_CASE` | `MAX_BLOCKS`, `HEADER_SIZE` | `MaxBlocks`            |

### Function Naming Rules

**Ban `get_` and `set_` prefixes:**

```zig
// Good: Noun for simple getters
fn capacity(self: *const Buffer) usize { ... }

// Good: Specific verb for mutators
fn update_capacity(self: *Buffer, new_capacity: usize) void { ... }

// Good: Action verbs for operations
fn find_block(self: *Engine, id: BlockId) !ContextBlock { ... }

// Bad: Generic get/set prefixes
fn get_capacity() usize { ... }
fn set_capacity() void { ... }
```

## Initialization Patterns

### Single-Phase (Preferred)

For simple components:

```zig
pub fn init(allocator: std.mem.Allocator, config: Config) Component {
    return Component{ .allocator = allocator, .config = config };
}
```

### Two-Phase (I/O Components)

For components requiring I/O:

```zig
pub fn init(allocator: std.mem.Allocator, vfs: VFS, data_dir: []const u8) !Component {
    // Phase 1: Memory allocation only, no I/O
    return Component{
        .allocator = allocator,
        .vfs = vfs,
        .data_dir = try allocator.dupe(u8, data_dir),
        .initialized = false,
    };
}

pub fn startup(self: *Component) !void {
    // Phase 2: I/O operations, can fail
    try self.vfs.mkdir(self.data_dir);
    try self.discover_existing_files();
    self.initialized = true;
}
```

**Rule**: `init()` never performs I/O. `startup()` handles resource discovery.

## Error Handling

### Specific Error Sets

No `anyerror` in public APIs:

```zig
// Good: Specific error contract
const StorageError = error{
    BlockNotFound,
    CorruptedData,
} || std.fs.File.ReadError;

pub fn find_block(id: BlockId) StorageError!ContextBlock { ... }

// Bad: Generic error type
pub fn find_block(id: BlockId) !ContextBlock { ... }
```

### Error Context

Use `error_context` for rich debugging:

```zig
if (computed_checksum != expected) {
    return error_context.storage_error(error.InvalidChecksum, .{
        .operation = "deserialize_block",
        .file_path = self.file_path,
        .expected = expected,
        .actual = computed_checksum,
    });
}
```

## Comments

### What to Comment

**Required**:

- Design rationale and trade-offs
- Performance decisions
- Non-obvious constraints
- Public API documentation (`///`)

**Forbidden**:

- Restating obvious code
- Step-by-step narration
- Development artifacts (`TODO`, `FIXME`, `HACK`)
- Commented-out code

### Examples

```zig
// Good: Explains design decision
// Linear scan outperforms binary search for <16 SSTables
// due to cache locality and reduced branching overhead
for (self.sstables.items) |sstable| { ... }

// Good: Non-obvious constraint
// Block must be cloned to prevent use-after-free when
// source MemTable is compacted and deallocated
const owned_block = try source_block.clone(allocator);

// Bad: Obvious statement
// Loop through SSTables
for (self.sstables.items) |sstable| { ... }

// Bad: Development noise
// TODO: optimize this later
const result = slow_operation();
```

## Testing Patterns

### Memory Management

Use `std.testing.allocator` (detects leaks automatically):

```zig
test "storage engine operations" {
    const allocator = std.testing.allocator;

    var engine = try StorageEngine.init(allocator, vfs, data_dir);
    defer engine.deinit();

    // Test operations...
}
```

### VFS Abstraction

All I/O through VFS for simulation testing:

```zig
test "handles file corruption" {
    var sim = try Simulation.init(allocator, seed);
    defer sim.deinit();

    // Simulate corruption
    try sim.vfs.inject_fault(.file_corruption, "wal_segment_01.log");

    // Test recovery behavior
    var engine = try StorageEngine.init(allocator, sim.vfs, data_dir);
    const result = engine.startup();
    try testing.expectError(error.CorruptedData, result);
}
```

### Deterministic Testing

Prefer simulation over mocking:

```zig
// Good: Real code, simulated environment
test "network partition during write" {
    var sim = try Simulation.init(allocator, seed);
    sim.network.partition_nodes(.{1, 2}, .{3});
    // Test with real storage engine...
}

// Bad: Mock objects
test "write failure" {
    var mock_storage = MockStorage.init();
    mock_storage.expect_write_failure();
    // Test behavior loses realism...
}
```

## Concurrency

### Single-Threaded Core

Use assertions to enforce threading model:

```zig
pub fn put_block(self: *StorageEngine, block: ContextBlock) !void {
    concurrency.assert_main_thread();  // Debug builds only
    // ... implementation
}
```

### Async I/O Only

Concurrency through event loop, not threads:

```zig
// Good: Async I/O operations
const write_task = async self.vfs.write(file, data);
const read_task = async self.vfs.read(other_file);
const write_result = await write_task;
const read_result = await read_task;

// Bad: Thread-based concurrency in core logic
const thread = try std.Thread.spawn(.{}, worker_function, args);
```

## Performance Guidelines

### Hot Path Optimizations

```zig
// Good: Linear scan for small collections
// Binary search overhead > linear scan for <16 items
for (self.sstables.items) |sstable| { ... }

// Good: Pre-allocated buffers
var buffer: [4096]u8 = undefined;
const result = try self.format_to_buffer(&buffer, block);

// Bad: Dynamic allocation in hot path
const formatted = try allocator.alloc(u8, estimated_size);
```

### Memory Efficiency

```zig
// Good: Arena for related allocations
var arena = std.heap.ArenaAllocator.init(backing_allocator);
defer arena.deinit();
const temp_allocator = arena.allocator();

// Process many related items using temp_allocator
// All memory freed at once with arena.deinit()

// Bad: Individual allocations
for (items) |item| {
    const processed = try allocator.alloc(u8, item.size);
    defer allocator.free(processed);  // Expensive
    // ...
}
```

## Defensive Programming

### Assertions

Use comprehensive assertions in debug builds:

```zig
const assert = @import("core/assert.zig");

pub fn put_block(self: *Engine, block: ContextBlock) !void {
    assert.not_null(block.content);
    assert.positive(block.content.len);
    assert.valid_block_id(block.id);

    // ... implementation
}
```

### Invariant Checking

Validate critical invariants:

```zig
pub fn compact_sstables(self: *SSTableManager) !void {
    const initial_count = self.sstables.items.len;

    // ... compaction logic

    // Invariant: Compaction reduces or maintains file count
    assert.less_than_or_equal(self.sstables.items.len, initial_count);
}
```

## Code Organization

### Module Boundaries

Clear dependency hierarchy:

```
core/           # Foundation, no dependencies
├── storage/    # Depends on core/
├── query/      # Depends on core/ and storage/
├── server/     # Depends on core/, storage/, query/
└── sim/        # Depends on core/ only (for testing)
```

### File Structure

```zig
// File header with module purpose
//! SSTableManager handles on-disk sorted string table operations.
//! Responsibilities: file discovery, compaction coordination, read optimization.

const std = @import("std");
const core = @import("../core/membank.zig");
const assert = @import("../core/assert.zig");

// Types first
const Self = @This();
const SSTable = @import("sstable.zig");

// Public API
pub const SSTableManager = struct {
    // Implementation...
};

// Tests at bottom
test "sstable manager lifecycle" {
    // Test implementation...
}
```

## Anti-Patterns

**Banned**:

- Global state
- Hidden allocations
- Thread-based concurrency in core logic
- `anyerror` in public APIs
- `get_`/`set_` function prefixes
- I/O in `init()` functions
- Mocking instead of simulation

**Required**:

- Explicit allocator parameters
- Specific error sets
- VFS abstraction for all I/O
- Arena-per-subsystem for complex state
- Comprehensive assertions in debug builds

## Style Enforcement

**Automated via `tidy`**:

- Custom assert usage (no `std.debug.assert`)
- Lifecycle naming conventions
- Error set specificity
- Comment quality

**Git hooks**:

- Code formatting (`zig fmt`)
- Commit message standards
- Test passage before commit

Membank style serves reliability. Every rule prevents real bugs.
