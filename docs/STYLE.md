# CortexDB Style Guide

## 1. Guiding Principles

This guide is not a list of pedantic rules. It is a codification of our architectural philosophy. Every rule exists to serve one of our four core principles. When in doubt, defer to these principles.

1.  **Clarity & Explicitness:** All control flow and memory allocations must be explicit. The code should do what it says, and nothing more. We avoid "magic" and hidden complexity.

2.  **Correctness & Robustness:** The system must be verifiably correct. We favor simple, predictable patterns over complex, clever ones. We use the type system and a strong assertion framework to make incorrect states unrepresentable.

3.  **Performance by Design:** Performance is achieved by architecting the system to avoid unnecessary work (e.g., dynamic allocations on hot paths), not by premature micro-optimization. A simple, data-oriented design is our primary performance tool.
4.  **Determinism & Testability:** All core logic must be runnable within the deterministic simulation framework. This requires abstracting all sources of non-determinism, such as I/O (VFS) and time.

## 2. Memory Management: The CortexDB Playbook

Memory management is the most critical aspect of our architecture. The following patterns are non-negotiable as they are fundamental to the stability and performance of the system.

### The Allocator is an Explicit Parameter

A function or struct that allocates memory must accept an `allocator: std.mem.Allocator` as a parameter. There are no global allocators.

```zig
// Good: Allocator dependency is explicit.
pub fn init(allocator: std.mem.Allocator) !MyStruct { ... }
```

### The Arena-per-Subsystem Pattern

For managing the lifetime of complex, related objects, we use arenas. A subsystem that owns a collection of dynamic data (like the `BlockIndex` memtable) owns an `ArenaAllocator` for all of that data.

This pattern eliminates manual `free()` calls for individual objects in favor of a single, atomic `arena.reset()`.

```zig
// In src/storage.zig

pub const BlockIndex = struct {
    blocks: std.HashMap(BlockId, ContextBlock, ...),

    // This arena owns ALL string memory for the blocks in the memtable.
    arena: std.heap.ArenaAllocator,

    pub fn init(allocator: std.mem.Allocator) BlockIndex {
        return .{
            .blocks = .init(allocator),
            .arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn put_block(self: *BlockIndex, block: ContextBlock) !void {
        // GOOD: No manual `free` needed. The arena's allocator is used
        // to dupe the strings, which are all freed together later.
        const arena_allocator = self.arena.allocator();
        const cloned_block = ContextBlock{
            .id = block.id,
            .version = block.version,
            .source_uri = try arena_allocator.dupe(u8, block.source_uri),
            .metadata_json = try arena_allocator.dupe(u8, block.metadata_json),
            .content = try arena_allocator.dupe(u8, block.content),
        };
        try self.blocks.put(block.id, cloned_block);
    }

    pub fn clear(self: *BlockIndex) void {
        self.blocks.clearRetainingCapacity();
        // GOOD: All string memory is freed at once. Simple, fast, and safe.
        _ = self.arena.reset(.retain_capacity);
    }
};
```

### No General-Purpose Allocations on Hot Paths

Critical performance paths (e.g., WAL writes, query execution, SSTable compaction) must not use a general-purpose allocator (`gpa.alloc()`).

-   For short-lived, temporary memory, a temporary `ArenaAllocator` should be passed into the function.
-   For frequently used buffers, a `BufferPool` can be considered, but only if profiling proves it is a significant bottleneck and a safer implementation (e.g., with allocation headers) is used.

## 3. Error Handling

### Use Specific, Scoped Error Sets

Functions must return specific error sets. Avoid returning a generic `anyerror` from high-level functions. Union specific error sets from dependencies to create a clear error contract.

```zig
// GOOD: The caller knows exactly what to expect.
const StorageError = error{
    BlockNotFound,
    CorruptedWALEntry,
} || std.fs.File.ReadError;

pub fn find_block_by_id(self: *StorageEngine, id: BlockId) StorageError!ContextBlock { ... }

// BAD: The caller has no idea what errors to handle.
pub fn find_block_by_id(self: *StorageEngine, id: BlockId) !ContextBlock { ... }
```

### Use `error_context` for Rich Debugging

When returning an error from a deep call stack, wrap it with `error_context` to provide rich diagnostic information in debug builds. This has zero performance cost in release builds.

```zig
// GOOD: Provides invaluable debugging info when things go wrong.
if (computed_checksum != header.checksum) {
    return error_context.storage_error(error.InvalidChecksum, .{
        .operation = "deserialize_block",
        .file_path = self.file_path,
        .expected_value = header.checksum,
        .actual_value = computed_checksum,
    });
}
```

## 4. Naming Conventions

Our naming conventions follow Zig standards but add specific constraints to enforce our design philosophy.

| Type             | Convention             | Good Examples                 | Bad Examples               |
| ---------------- | ---------------------- | ----------------------------- | -------------------------- |
| **Types**        | `PascalCase`           | `StorageEngine`, `BlockId`    | `storage_engine`           |
| **Constants**    | `SCREAMING_SNAKE_CASE` | `MAX_BLOCKS`, `HEADER_SIZE`   | `MaxBlocks`, `header_size` |
| **Functions**    | `snake_case`           | `find_block`, `serialize`     | `findBlock`, `GetBlock`    |
| **Variables**    | `snake_case`           | `block_count`, `wal_file`     | `blockCount`, `WALFile`    |

**CRITICAL: Function Naming Philosophy**

We strictly forbid `get_` and `set_` prefixes for function names. This enforces a clearer separation between simple accessors and operations with side effects.

-   **For simple getters:** Use a noun.
    -   `fn id(self: *const Block) u64`
    -   `fn capacity(self: *const Buffer) usize`
-   **For setters/mutators:** Use a specific verb that describes the action.
    -   `fn update_id(self: *Block, id: u64)`
    -   `fn clear(self: *BlockIndex)`
-   **For operations:** Use a verb that describes the work being done.
    -   `fn find_block_by_id(...)`
    -   `fn recover_from_wal(...)`

## 5. Commenting Philosophy: The WHY, Not the WHAT

Comments should explain the **rationale, design decisions, and non-obvious constraints** of the code. We assume a competent reader; do not explain what the code is doing if it's obvious.

```zig
// BAD: This comment is useless and just adds noise.
// Increment the index.
i += 1;

// GOOD: This comment explains a non-obvious design decision.
// We use a linear scan of the index here instead of binary search.
// For the expected number of SSTables (< 16), the simpler code and
// improved cache locality of a linear scan outweighs the algorithmic
// benefit of a binary search.
for (self.index.items) |entry| { ... }
```

-   Public APIs must have full `///` documentation explaining their purpose, parameters, and potential errors.
-   `TODO` and `FIXME` comments are for local development only and **must be removed** before merging into `main`.

## 6. Testing

### Deterministic Tests are the Default

The default for any new feature or bug fix is a deterministic simulation test. Unit tests are for simple, pure functions. System behavior must be validated in the simulation framework.

### Arena-per-Test for Guaranteed Isolation

Every `test` block must create its own `ArenaAllocator`. This is a non-negotiable rule to prevent the cumulative memory corruption issues we have previously faced.

```zig
test "storage engine can write and read a block" {
    // GOOD: This test is perfectly isolated from all others.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var sim = try Simulation.init(allocator, 1234);
    var storage = try StorageEngine.init(allocator, ...);
    // ... rest of test logic ...
}
```
