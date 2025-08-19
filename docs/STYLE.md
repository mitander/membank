# Code Style

Disciplined code for microsecond-scale reliability. These patterns eliminate bugs and enforce architecture.

## Naming: Verbs Do, Nouns Are

### Functions Start With Verbs

```zig
// GOOD: Action-first, unambiguous
pub fn find_block(id: BlockId) !?ContextBlock
pub fn flush_memtable() !void
pub fn validate_checksum(data: []const u8) bool

// BAD: Unclear intent
pub fn block_finder(id: BlockId) !?ContextBlock
pub fn checksum(data: []const u8) bool
```

### Uncertainty Prefixes

**Error unions**: `try_parse_header()`, `try_connect()`

**Optionals**: `maybe_find_block()`, `maybe_get_cached()`

### Lifecycle Methods

Standard names only:

```zig
pub fn init()      // COLD: Memory allocation only
pub fn startup()   // HOT: I/O operations, resource acquisition
pub fn shutdown()  // Graceful termination
pub fn deinit()    // Memory cleanup
```

**Forbidden**: `initialize()`, `start()`, `stop()`, `cleanup()`

### No Get/Set Prefixes

```zig
// BAD
pub fn get_block_count() u32
pub fn set_memory_limit(limit: u64) void

// GOOD
pub fn block_count() u32
pub fn update_memory_limit(limit: u64) void
```

## Comments: Why, Not What

Code is self-documenting. Comments explain reasoning, trade-offs, and constraints.

```zig
// BAD: Explaining the obvious
// Increment the counter
counter += 1;

// GOOD: Explaining the reasoning
// WAL requires sequential entry numbers for recovery validation.
// Batch incrementing would break ordering guarantees.
counter += 1;
```

### Required Comments

- **Design rationale**: Why this approach over
  alternatives
- **Performance trade-offs**: Why simpler beats complex
- **Protocol constraints**: External requirements
- **Safety guarantees**: When using unsafe operations

### Forbidden Comments

- Step-by-step narration
- Commented-out code
- TODO/FIXME/HACK markers
- Obvious operations

## Memory: Arena Coordinator Pattern

Coordinators provide stable interfaces that survive arena operations.

```zig
pub const StorageEngine = struct {
    storage_arena: ArenaAllocator,
    coordinator: ArenaCoordinator,  // Stable interface

    pub fn flush_memtable(self: *StorageEngine) !void {
        // ... flush to disk ...
        self.coordinator.reset();  // All memory gone in O(1)
    }
};
```

**Rules:**

- Coordinators own exactly one arena
- Submodules receive coordinator interfaces
- Never embed arenas in copyable structs
- O(1) cleanup via coordinator.reset()

## Error Handling

**Explicit handling**:

```zig
// BAD
const file = try vfs.open(path) unreachable;

// GOOD
const file = vfs.open(path) catch |err| {
    log.err("Failed to open {s}: {}", .{ path, err });
    return StorageError.FileNotFound;
};
```

**Assertions**:

```zig
assert.positive(x)           // Debug only
fatal_assert(x > 0, "msg")   // Always active
```

## Code Organization

**Import order**:

1. Standard library
2. Core modules
3. Subsystem modules
4. Test utilities

**Error types**:

```zig
pub const StorageError = error{
    BlockNotFound,
    ChecksumMismatch,
    CorruptedHeader,
};
```

## Testing

**Harness-first rule**: Use standardized harnesses unless technically impossible.

```zig
// GOOD: Use harness
var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "test_db");
defer harness.deinit();

// Manual setup requires explicit justification:
// Manual setup required because: Recovery testing needs two separate
// StorageEngine instances sharing the same VFS to validate WAL recovery
```

**Test naming**: Describe scenario and outcome

```zig
test "put_block rejects empty content" { }
test "recovery handles corrupted WAL entries" { }
```

## Commit Messages

```
type(scope): brief summary

Context explaining the problem and solution. (Optional)

- Specific change 1
- Specific change 2

Impact: Brief result statement. (Optional)
```

## Enforcement

- Pre-commit hooks: Automatic formatting
- Tidy checker: Naming convention validation
- Code review: Manual verification
- CI pipeline: Style checking

Violations cause build failures and review rejection.
