# Style Guide

This document defines the mandatory code style, naming conventions, and patterns for KausalDB. These rules enforce architectural principles, improve readability, and eliminate entire classes of bugs. Adherence is enforced by the tidy checker and pre-commit hooks.

## 1. Core Philosophy: Code Shows WHAT, Comments Explain WHY

### The Fundamental Rule

- **Code must be self-documenting**: A competent developer should understand WHAT the code does by reading it.
- **Comments provide context**: Comments explain WHY decisions were made, not WHAT the code does.

```zig
// BAD: Comment explains what (redundant)
// Increment the counter
counter += 1;

// GOOD: Comment explains why (valuable context)
// We increment by 1 instead of batch size here because the WAL
// requires sequential entry numbers for recovery validation
counter += 1;
```

## 2. Naming Conventions: Verb-First and Explicit

### Function Names: Actions Are Verbs

Functions perform actions. Their names must start with a verb that clearly indicates what they do.

```zig
// GOOD: Clear action verbs
pub fn find_block(id: BlockId) !?ContextBlock
pub fn flush_memtable() !void
pub fn validate_checksum(data: []const u8) bool
pub fn compact_sstables(level: u32) !void

// BAD: Noun-first or ambiguous
pub fn block_finder(id: BlockId) !?ContextBlock  // Noun, not verb
pub fn memtable_flush() !void                     // Noun-first
pub fn checksum(data: []const u8) bool           // Ambiguous - calculate or validate?
```

### Special Verb Prefixes

**Result Types**: Functions returning error unions use `try_` prefix:

```zig
pub fn try_parse_header(data: []const u8) !Header
pub fn try_connect(address: []const u8) !Connection
```

**Optional Types**: Functions returning optionals use `maybe_` prefix:

```zig
pub fn maybe_find_block(id: BlockId) ?ContextBlock
pub fn maybe_get_cached_result(key: []const u8) ?QueryResult
```

### NO Get/Set Prefixes

Direct field access is preferred over trivial getters/setters. When accessor logic is needed, use descriptive verbs.

```zig
// BAD: Unnecessary get/set prefixes
pub fn get_block_count() u32
pub fn set_memory_limit(limit: u64) void

// GOOD: Direct access or descriptive verbs
pub fn block_count() u32              // Simple accessor
pub fn update_memory_limit(limit: u64) void  // Shows it does more than just set
```

### Lifecycle Methods: Standard Names Only

Component lifecycle must use these exact names:

```zig
// REQUIRED lifecycle sequence:
pub fn init(allocator: Allocator) Self        // COLD: Memory allocation only
pub fn startup() !void                        // HOT: I/O operations, resource acquisition
pub fn run() !void                            // Private blocking event loop
pub fn shutdown() !void                       // Graceful termination
pub fn deinit() void                         // Memory cleanup

// FORBIDDEN legacy names:
initialize()  // Use init() or startup()
start()       // Use startup()
stop()        // Use shutdown()
cleanup()     // Use deinit()
destroy()     // Use deinit()
```

### Variable and Field Names

**State and Data**: Use nouns that describe what they contain:

```zig
// GOOD: Clear, descriptive nouns
block_count: u32
memory_used: u64
is_running: bool
wal_segments: []WALSegment

// BAD: Ambiguous or abbreviated
cnt: u32        // Unclear abbreviation
mem: u64        // Ambiguous - memory what?
running: bool   // Missing 'is_' prefix for boolean
segments: []WALSegment  // Which segments?
```

**Booleans**: Prefix with `is_`, `has_`, `should_`, or `can_`:

```zig
is_initialized: bool
has_pending_writes: bool
should_compact: bool
can_read: bool
```

### Constants and Enums

**Constants**: SCREAMING_SNAKE_CASE for compile-time constants:

```zig
const MAX_BLOCK_SIZE = 1024 * 1024;  // 1MB
const WAL_MAGIC = 0x57414C4F;        // 'WALO'
const DEFAULT_TIMEOUT_MS = 5000;
```

**Enums**: PascalCase for type, lowercase for values:

```zig
const StorageState = enum {
    uninitialized,
    initializing,
    running,
    flushing,
    compacting,
    stopping,
    stopped,
};
```

## 3. Comment Standards: Context Over Description

### Required Comments

**Design Rationale**: Explain non-obvious technical decisions:

```zig
// We use linear scan instead of binary search for SSTables.
// With typical counts (<16), cache locality and reduced branching
// outperform the O(log n) algorithmic advantage of binary search.
for (self.sstables) |sstable| { ... }
```

**Performance Trade-offs**: Document why you chose this approach:

```zig
//
```

Arena allocation chosen over pooling despite fragmentation risk.
// Our workload's predictable allocation patterns and bulk deallocation
// during flush operations make arena's O(1) cleanup more valuable
// than pool's fragmentation resistance.
const arena = ArenaAllocator.init(allocator);

````

**Protocol/Specification Requirements**: External constraints:
```zig
// CRC-64 required by storage format v2 specification.
// Do not change to CRC-32 even though it would be faster -
// backward compatibility depends on this exact algorithm.
const checksum = crc64(data);
````

**Safety Guarantees**: When using unsafe operations:

```zig
// Safety: Buffer size verified by caller through bounds check at L342.
// The slice operation cannot panic because we validate size >= offset.
const data = buffer[offset..];
```

**Workarounds and Technical Debt**: Temporary solutions:

```zig
// WORKAROUND: Using mutex here due to Zig issue #12345.
// Should be lock-free once atomic compare-exchange is fixed.
// Tracking issue: https://github.com/ziglang/zig/issues/12345
var mutex = std.Thread.Mutex{};
```

### Forbidden Comments

**Never explain what code obviously does**:

```zig
// BAD: These comments add no value
i += 1;  // Increment i
if (x > 0) { }  // Check if x is positive
return result;  // Return the result

// BAD: Step-by-step narration
// First, we open the file
const file = try openFile(path);
// Then we read the data
const data = try file.read();
// Finally we close the file
file.close();
```

**Never leave commented-out code**:

```zig
// BAD: Creates confusion and clutter
// old_implementation();
new_implementation();
```

**Never use TODO/FIXME/HACK comments**:

```zig
// BAD: Fix it or track it properly
// TODO: Optimize this later
// FIXME: Handle edge case
// HACK: Temporary workaround
```

### Exception: Test Narratives

Tests may use descriptive comments to create readable scenarios:

```zig
test "WAL recovery handles corruption gracefully" {
    // Setup: Create WAL with valid entries
    var wal = try WAL.init(allocator, test_dir);
    try wal.write_entry(valid_entry_1);
    try wal.write_entry(valid_entry_2);

    // Inject corruption into the third entry's checksum
    try test_file.seekTo(entry_3_offset);
    try test_file.writer().writeInt(u64, 0xDEADBEEF, .big);

    // Recovery should restore valid entries and skip corrupted one
    var recovered_wal = try WAL.recover(allocator, test_dir);
    try testing.expectEqual(@as(u32, 2), recovered_wal.entry_count());
}
```

## 4. Memory Management: Arena-per-Subsystem

### Ownership Rules

Every allocation must have clear ownership:

```zig
pub const MemtableManager = struct {
    // Arena owns all memtable allocations
    arena: ArenaAllocator,
    // Backing allocator for stable structures (HashMap itself)
    backing_allocator: Allocator,

    pub fn put_block(self: *Self, block: ContextBlock) !void {
        // Clone into arena for clear ownership
        const owned_block = try self.arena.dupe(u8, block.content);
        // NEVER store pointers to external memory
    }
};
```

### Zero-Cost Abstractions

Safety mechanisms must have zero runtime cost in release builds:

```zig
// Ownership validation only in debug builds
pub fn read(self: *const OwnedBlock, comptime accessor: BlockOwnership) *const ContextBlock {
    if (comptime builtin.mode == .Debug) {
        self.validate_ownership(accessor);
    }
    return &self.block;
}
```

## 5. Error Handling: Explicit and Fail-Fast

### Error Union Handling

**Never use `try unreachable`**. Handle errors explicitly or document safety:

```zig
// BAD: Hides potential failures
const file = try vfs.open(path) unreachable;

// GOOD: Explicit error handling
const file = vfs.open(path) catch |err| {
    log.err("Failed to open {s}: {}", .{ path, err });
    return StorageError.FileNotFound;
};

// GOOD: Documented safety guarantee
// Safety: VFS lifecycle guarantees file exists after successful create()
const file = vfs.open(path) catch unreachable;
```

### Assertion Categories

**Debug Assertions**: Development-time validation (compiled out in release):

```zig
assert.positive(block_size);
assert.not_null(allocator);
assert.less_than(index, array.len);
```

**Fatal Assertions**: Unrecoverable corruption detection (always active):

```zig
// Memory accounting corruption is unrecoverable
fatal_assert(self.memory_used >= size,
    "Memory underflow: {} < {} - heap corruption",
    .{ self.memory_used, size });
```

## 6. Performance Considerations

### Hot Path Rules

In performance-critical paths (sub-microsecond operations):

1. **Zero allocations**: Use stack allocation or pre-allocated buffers
2. **No runtime validation**: Only compile-time checks
3. **Inline small functions**: Use `inline` keyword judiciously
4. **Document assumptions**: Explain why safety checks are skipped

```zig
// Hot path: Block lookup must be <0.1µs
pub inline fn find_block_fast(self: *const BlockIndex, id: BlockId) ?*const ContextBlock {
    // No allocation: HashMap lookup only
    // No validation: Caller ensures valid state
    // Safety: BlockIndex.init() guarantees map is initialized
    return if (self.blocks.get(id)) |entry| &entry.block else null;
}
```

## 7. Code Organization

### Import Order

1. Standard library
2. Core modules (types, assert, ownership)
3. Subsystem modules (storage, query, network)
4. Test utilities (testing only)

```zig
const std = @import("std");
const builtin = @import("builtin");

const assert = @import("../core/assert.zig");
const types = @import("../core/types.zig");
const ownership = @import("../core/ownership.zig");

const StorageEngine = @import("storage/engine.zig").StorageEngine;
const QueryEngine = @import("query/engine.zig").QueryEngine;
```

### Error Types

Group related errors into categorical error sets:

```zig
pub const StorageError = error{
    BlockNotFound,
    ChecksumMismatch,
    CorruptedHeader,
    DiskFull,
    IOError,
};

pub const NetworkError = error{
    ConnectionRefused,
    Timeout,
    ProtocolError,
};
```

## 8. Testing Standards

### Test Naming

Test names must describe the scenario and expected outcome:

```zig
test "put_block rejects empty content" { }
test "memtable flush preserves all blocks" { }
test "recovery handles corrupted WAL entries" { }
```

### Test Structure

Follow Arrange-Act-Assert pattern:

```zig
test "compaction merges overlapping SSTables" {
    // Arrange: Setup test state
    var storage = try StorageEngine.init(allocator);
    defer storage.deinit();

    // Act: Perform operation
    try storage.compact_level(0);

    // Assert: Verify outcome
    try testing.expectEqual(@as(u32, 1), storage.sstable_count());
}
```

### Test Setup Patterns

**Harness-First Rule**: Tests MUST use standardized harnesses unless manual setup is technically required.

**Preferred Pattern**:

```zig
test "storage operations" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "test_db");
    defer harness.deinit();

    try harness.storage_engine.put_block(test_block);
}
```

**Manual Setup Justification**: When harnesses cannot be used, provide explicit justification:

```zig
// Manual setup required because: [specific technical reason]
// [Additional context explaining why harness doesn't work]
var sim_vfs = try SimulationVFS.init(allocator);
defer sim_vfs.deinit();
```

**Required Justification Patterns**:

**Recovery Testing**:

```zig
// Manual setup required because: Recovery testing needs two separate
// StorageEngine instances sharing the same VFS to validate WAL recovery
// across engine lifecycle. StorageHarness is designed for single-engine scenarios.
```

**Fault Injection Timing**:

```zig
// Manual setup required because: Test needs to insert data first, then enable
// I/O failures to simulate corruption during reads. FaultInjectionHarness
// applies faults during startup(), which would prevent initial data insertion.
```

**Performance Measurement**:

```zig
// Manual setup required because: Performance tests need specific allocator
// configurations for measurement isolation. Harness arena allocation would
// interfere with accurate memory usage tracking.
```

**Memory Safety Testing**:

```zig
// Manual setup required because: Memory safety validation requires
// GeneralPurposeAllocator with safety features enabled to detect
// buffer overflows and use-after-free errors.
```

**Good vs Bad Test Patterns**:

**BAD: Manual setup without justification**:

```zig
test "block operations" {
    const allocator = testing.allocator;

    // BAD: Manual VFS/StorageEngine setup without justification
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "test_db");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // BAD: Custom block creation instead of TestData utilities
    const content = try std.fmt.allocPrint(allocator, "test content {}", .{42});
    defer allocator.free(content);
    const block = ContextBlock{
        .id = TestData.deterministic_block_id(42),
        .version = 1,
        .source_uri = "test://example.zig",
        .metadata_json = "{}",
        .content = content,
    };

    try storage_engine.put_block(block);
}
```

**GOOD: Harness usage with standardized utilities**:

```zig
test "block operations" {
    const allocator = testing.allocator;

    // GOOD: Use StorageHarness for coordinated setup
    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "test_db");
    defer harness.deinit();

    // GOOD: Use standardized TestData utilities
    const content = try std.fmt.allocPrint(allocator, "test content {}", .{42});
    defer allocator.free(content);
    const block = try TestData.create_test_block_with_content(allocator, 42, content);
    defer {
        allocator.free(block.source_uri);
        allocator.free(block.metadata_json);
        allocator.free(block.content);
    }

    try harness.storage_engine.put_block(block);
}
```

**ACCEPTABLE: Manual setup with proper justification**:

```zig
test "wal recovery across storage engines" {
    const allocator = testing.allocator;

    // Manual setup required because: Recovery testing needs two separate
    // StorageEngine instances sharing the same VFS to validate WAL recovery
    // across engine lifecycle. StorageHarness is designed for single-engine scenarios.
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    // First engine writes data
    var storage_engine1 = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "test_db");
    defer storage_engine1.deinit();
    try storage_engine1.startup();

    const block = try TestData.create_test_block(allocator, 42);
    defer {
        allocator.free(block.source_uri);
        allocator.free(block.metadata_json);
        allocator.free(block.content);
    }
    try storage_engine1.put_block(block);

    // Second engine recovers data using same VFS
    var storage_engine2 = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "test_db");
    defer storage_engine2.deinit();
    try storage_engine2.startup();

    const recovered = try storage_engine2.find_block(block.id);
    try testing.expect(recovered != null);
}
```

## 9. Commit Message Standards

Follow Conventional Commits format:

```
type(scope): brief summary

Context explaining the problem and solution.

- Specific change 1
- Specific change 2
- Specific change 3

Impact: Brief statement of the result.
```

Example:

```
fix(storage): resolve arena allocation alignment panic

TraversalResult allocations were misaligned due to PackedBlockPointer
size not being a power of 2, causing crashes on ARM64.

- Add explicit alignment to PackedBlockPointer struct
- Ensure all arena allocations use natural alignment
- Add alignment assertion in debug builds

Impact: Eliminates alignment panics in CI builds.
```

## 10. Enforcement

These standards are enforced through:

1. **Pre-commit hooks**: Automatic formatting and validation
2. **Tidy checker**: Naming convention enforcement
3. **Code review**: Manual verification of comment quality
4. **CI pipeline**: Automated style checking

Violations will cause:

- Pre-commit hook failures (local)
- CI build failures (pull requests)
- Code review rejection (manual review)

Remember: These rules exist to make the codebase maintainable, performant, and correct. When in doubt, optimize for clarity and simplicity.
