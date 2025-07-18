# CortexDB Style Guide

Code style and conventions for CortexDB development.

## Core Principles

- **Explicit over clever**: Code should be immediately readable
- **Consistent patterns**: Follow established conventions throughout
- **Performance aware**: Consider allocations and hot paths
- **Defensive programming**: Assert invariants liberally
- **Deterministic testing**: All code must work in simulation

## Naming Conventions

### Functions

Use `verb_noun` pattern for actions:

```zig
pub fn validate_block(block: *const Block) !void
pub fn process_query(query: []const u8) !QueryResult
pub fn encode_header(header: *Header, buffer: []u8) !void
```

Use nouns for getters:

```zig
pub fn block_count(self: *const Storage) u32
pub fn query_timeout(self: *const Engine) u64
```

Use `is_` prefix for boolean queries:

```zig
pub fn is_valid(block: *const Block) bool
pub fn is_corrupted(header: *const Header) bool
```

Generic functions must end with `Type`:

```zig
pub fn BoundedArrayType(comptime T: type, comptime capacity: usize) type
pub fn HashMapType(comptime K: type, comptime V: type) type
```

### Variables

Use `snake_case` for all variables:

```zig
const block_hash = calculate_hash(data);
var connection_pool = ConnectionPool.init(allocator);
```

**Exceptions:** Conventional names from standard library and common patterns:

```zig
const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const fs = std.fs;
const os = std.os;
const log = std.log;
```

Use `maybe_` prefix for optional types:

```zig
const maybe_parent_id: ?u64 = block.parent_id;
var maybe_cached_result: ?QueryResult = cache.get(key);
```

Use `try_` prefix for result types:

```zig
const try_connection = pool.acquire();
const try_parsed_query = parse_query(input);
```

### Constants

Use `SCREAMING_SNAKE_CASE`:

```zig
const MAX_BLOCK_SIZE = 1024 * 1024;
const DEFAULT_TIMEOUT_MS = 5000;
const HASH_SIZE = 32;
```

### Types

Use `PascalCase` for types:

```zig
const QueryEngine = struct { ... };
const BlockStorage = struct { ... };
const ValidationError = error { ... };
```

## Code Structure

### Function Order

```zig
// 1. Public interface first
pub fn process_query(self: *QueryEngine, query: []const u8) !QueryResult {
    try validate_query_syntax(query);
    return execute_query(self, query);
}

// 2. Private helpers after
fn validate_query_syntax(query: []const u8) !void {
    if (query.len == 0) return error.EmptyQuery;
}

// 3. Types at the end
const QueryResult = struct {
    blocks: []const Block,
    count: u32,
};
```

### Error Handling

Define specific error sets:

```zig
const ValidationError = error{
    InvalidBlockHash,
    BlockTooLarge,
    MissingDependency,
};

pub fn validate_block(block: *const Block) ValidationError!void {
    assert(block.id != 0); // Programming error

    if (block.size > MAX_BLOCK_SIZE) {
        return ValidationError.BlockTooLarge;
    }
}
```

### Memory Management

Always pass allocators explicitly:

```zig
pub fn create_index(allocator: std.mem.Allocator, blocks: []const Block) !*Index {
    const index = try allocator.create(Index);
    errdefer allocator.destroy(index);

    try index.init(allocator, blocks);
    return index;
}
```

Use arena allocators for temporary data:

```zig
pub fn process_batch(allocator: std.mem.Allocator, blocks: []const Block) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const temp_buffer = try arena.allocator().alloc(u8, BUFFER_SIZE);
    // temp_buffer automatically freed
}
```

## Documentation

### Function Documentation

Keep documentation concise and practical:

```zig
/// Validates block integrity and dependencies.
/// Returns ValidationError if block is invalid.
pub fn validate_block(block: *const Block, state: *const State) ValidationError!void {
    // Implementation
}
```

### Module Documentation

```zig
//! Query engine for CortexDB context retrieval.
//!
//! Provides semantic and structural query processing with deterministic
//! behavior for testing and production environments.

const std = @import("std");
const assert = std.debug.assert;
```

### Inline Comments

Focus on "why" not "what":

```zig
// Use binary search since blocks are sorted by ID at construction time
const index = binary_search_block_id(blocks, target_id);

// Edge case: empty queries return all blocks up to limit
if (query.len == 0) {
    return blocks[0..@min(blocks.len, DEFAULT_LIMIT)];
}
```

## Testing

### Test Structure

```zig
test "validate_block: rejects invalid hash" {
    const allocator = std.testing.allocator;

    var block = Block{
        .id = 1,
        .data = "test",
        .hash = 0xDEADBEEF, // Wrong hash
    };

    try std.testing.expectError(
        ValidationError.InvalidBlockHash,
        validate_block(&block, &empty_state)
    );
}
```

### Deterministic Testing

```zig
test "query_engine: handles partition during query" {
    const allocator = std.testing.allocator;

    var sim = try Simulation.init(allocator, .{ .seed = 42 });
    defer sim.deinit();

    try sim.create_nodes(3);
    sim.partition_network(.{ .duration_ms = 1000 });

    const result = try sim.execute_query("blocks");
    try std.testing.expect(result.count > 0);
}
```

## Assertions

Use assertions for programming invariants:

```zig
pub fn get_block_at_index(self: *Storage, index: usize) *Block {
    assert(index < self.blocks.len);
    assert(self.blocks[index].id != 0);
    return &self.blocks[index];
}
```

## Performance

Document complexity and allocation patterns:

```zig
/// Binary search for block by ID.
/// Time: O(log n), Space: O(1)
pub fn find_block_by_id(blocks: []const Block, id: u64) ?*const Block {
    var left: usize = 0;
    var right: usize = blocks.len;

    while (left < right) {
        const mid = left + (right - left) / 2;
        // ... implementation
    }

    return null;
}
```

Avoid allocations in hot paths:

```zig
// Use stack buffer for small operations
var buffer: [256]u8 = undefined;
const result = try format_header(&buffer, header);
```

## Banned Patterns

- `std.BoundedArray` → use `stdx.BoundedArrayType`
- `std.StaticBitSet` → use `stdx.BitSetType`
- `@memcpy` → use `stdx.copy_disjoint`
- `Self = @This()` → use explicit type names
- `debug.assert` → use unqualified `assert`
- `FIXME` comments → must be resolved before merge
- `dbg()` calls → must be removed before merge

**Allowed Conventional Names:** `std`, `assert`, `mem`, `fs`, `os`, `log`, and other standard library imports are exceptions to snake_case rules.

## Formatting

- Use `zig fmt` for all code
- Maximum 100 characters per line
- No trailing whitespace
- 4-space indentation (enforced by `zig fmt`)

## File Organization

```
src/
├── main.zig           # Entry point
├── query_engine.zig   # Query processing
├── block_storage.zig  # Block storage
├── test_*.zig        # Test files
└── simulation/       # Simulation tests
```

This style guide is enforced by the `tidy` test and must be followed by all contributors.
