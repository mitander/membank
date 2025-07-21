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

Use contextual `verb_noun` pattern for actions:

```zig
/// Validates block integrity and dependencies.
pub fn validate_block(block: *const Block) !void
/// Processes query and returns results.
pub fn process_query(query: []const u8) !QueryResult
/// Encodes header into provided buffer.
pub fn encode_header(header: *Header, buffer: []u8) !void
/// Verifies cryptographic signature.
pub fn verify_signature(signature: []const u8) !bool
/// Receives message from connection.
pub fn receive_message(connection: *Connection) !Message
/// Updates block metadata.
pub fn update_metadata(self: *Block, metadata: Metadata) void
```

**BANNED:** `get_` and `set_` prefixes. Use naming conventions with contextual naming:

```
// BAD: get_/set_ pattern
get_id(block) -> u64
get_block(table, id) -> ?Block
get_config(engine) -> Config
get_file_size(file) -> u64
set_id(block, id)
set_config(engine, config)

// GOOD: contextual naming
id(block) -> u64                     // simple property getter
find_block(table, id) -> ?Block      // search operation
load_config(engine) -> Config        // data loading operation
file_size(file) -> u64               // computed property
update_id(block, id)                 // modify existing value
configure_engine(engine, config)     // setup operation
```

**Decision Guide for `get_` Replacements:**

- **Simple properties:** Use nouns → `id()`, `timestamp()`, `capacity()`
- **Search operations:** Use `find_` → `find_block()`, `find_user()`
- **Data loading:** Use `load_` → `load_config()`, `load_metadata()`
- **Computed values:** Use descriptive nouns → `file_size()`, `hash_code()`
- **Complex retrieval:** Use context → `fetch_from_cache()`, `read_from_disk()`

Use nouns for getters (no `get_` prefix):

```zig
/// Returns total number of blocks in storage.
pub fn block_count(self: *const Storage) u32
/// Returns query timeout in milliseconds.
pub fn query_timeout(self: *const Engine) u64
/// Returns block identifier.
pub fn id(self: *const Block) u64
/// Returns event timestamp.
pub fn timestamp(self: *const Event) u64
/// Returns buffer capacity.
pub fn capacity(self: *const Buffer) usize
```

Use `is_` prefix for boolean queries:

```zig
/// Returns true if block passes validation checks.
pub fn is_valid(block: *const Block) bool
/// Returns true if header has corruption indicators.
pub fn is_corrupted(header: *const Header) bool
/// Returns true if block depends on given dependency ID.
pub fn has_dependency(block: *const Block, dep_id: u64) bool
/// Returns true if query can be executed in current state.
pub fn can_execute(query: *const Query) bool
```

#### Approved Verb Prefixes

**Lifecycle:** `create_`, `destroy_`, `init_`, `deinit_`, `open_`, `close_`, `start_`, `stop_`

**Actions:** `process_`, `execute_`, `handle_`, `perform_`, `apply_`, `revert_`

**Validation:** `validate_`, `verify_`, `check_`, `ensure_`, `confirm_`

**I/O:** `read_`, `write_`, `load_`, `save_`, `send_`, `receive_`, `acquire_`, `release_`

**Transforms:** `parse_`, `encode_`, `decode_`, `serialize_`, `format_`, `convert_`

**Updates:** `update_` (modify existing), `modify_` (change state), `insert_` (add new), `remove_` (take away), `delete_` (destroy), `append_` (add to end), `configure_` (setup), `apply_` (execute changes)

**Search:** `find_`, `search_`, `lookup_`, `locate_`, `discover_`

**Results:** `try_`, `maybe_`, `as_`, `into_`, `to_`, `from_`

Generic functions must end with `Type`:

```zig
/// Creates bounded array type with compile-time capacity.
pub fn BoundedArrayType(comptime T: type, comptime capacity: usize) type
/// Creates hash map type for given key-value types.
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
pub fn block_at_index(self: *Storage, index: usize) *Block {
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

### Style Violations

- `get_*` functions → use noun getters: `id()` not `get_id()`, `find_block()` not `get_block()`
- `set_*` functions → use contextual verbs: `update_id()` not `set_id()`, `configure_engine()` not `set_config()`
- camelCase names → use snake_case consistently

**Contextual Verb Selection Guidelines:**

- Use `update_` for modifying existing values: `update_timestamp()`, `update_metadata()`
- Use `configure_` for setup/initialization: `configure_pool()`, `configure_limits()`
- Use `find_` for search operations: `find_block()`, `find_node()`
- Use `apply_` for executing changes: `apply_migration()`, `apply_patch()`
- Use `modify_` for state changes: `modify_permissions()`, `modify_behavior()`

### General Bans

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
