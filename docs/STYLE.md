# Obsessive Style Disorder

This document defines the mandatory style and patterns for all code in the Kausal project. These rules are enforced by `tidy` checks and code review.

## 1. Guiding Principle: "Why, Not What"

Comments exist to explain **rationale, design decisions, and non-obvious constraints**. We never explain what the code does; we explain why it does it that way. The code itself is the "what."

### Forbidden Comment Patterns

Writing comments that narrate the code is a critical violation. The reader is assumed to be a competent developer.

```zig
// BAD: Explains what the code obviously does.
// Set the value to 42.
value = 42;

// BAD: Restates the function call.
// Call the cleanup function.
cleanup();

// BAD: Commented-out code. Must be deleted.
// old_implementation();
new_implementation();

// BAD: Development noise. Must be resolved before commit.
// TODO: fix this later
// FIXME: handle this edge case
```

### Required Comment Patterns

Comments are mandatory for explaining non-obvious design choices.

```zig
// GOOD: Explains a non-obvious performance trade-off.
// We use linear scan instead of binary search here. For the expected
// number of SSTables (< 16), the simpler code and improved cache
// locality outweigh the O(log n) algorithmic benefit.
for (self.index.items) |entry| { ... }

// GOOD: Documents a critical correctness invariant.
// The block must be cloned here to prevent a use-after-free when the
// source MemTable is compacted and its arena is deallocated.
const owned_block = try source_block.clone(allocator);

// GOOD: Explains a constraint imposed by an external specification.
// Per the protocol specification, the checksum must cover the header
// and payload, but excludes the checksum field itself.
const checksum = calculate_crc32(header_bytes, payload_bytes);
```

## 2. Naming Conventions

*   **Types & Structs:** `PascalCase` (`BlockIndex`, `StorageEngine`).
*   **Functions & Variables:** `snake_case` (`put_block`, `block_count`).
*   **Global Constants:** `SCREAMING_SNAKE_CASE` (`MAX_PATH_LENGTH`).
*   **Banned Prefixes:** Functions must not use `get_` or `set_` prefixes. Use descriptive nouns (`block_count`) or verbs (`find_block`).

## 3. Error Handling

*   **Error Context:** All errors returned from I/O or other fallible operations must be wrapped with the helpers in `src/core/error_context.zig` to provide rich debugging information in debug builds.
*   **Unreachable Code:** The use of `catch unreachable` is forbidden unless accompanied by a `// Safety:` comment on the preceding line that rigorously justifies why the error condition is logically impossible to reach.

## 4. Memory Management

*   **Arena-per-Subsystem:** State-heavy subsystems (e.g., `BlockIndex`) must use an `ArenaAllocator` to manage the lifecycle of their internal data structures. This enables O(1) cleanup and prevents memory leaks.
*   **`defer` is Mandatory:** All local resources (memory, file handles) must be managed with `defer`. Manual cleanup is a violation.

## 5. Commit Messages

All commit messages must adhere to the **Conventional Commits** specification.

**Format:** `type(scope): summary`

**Example:**
```
fix(storage): resolve HashMap corruption via arena allocator

Memory corruption from mixed PooledAllocator/backing allocator usage
caused sporadic crashes after ~15 test cycles.

- Remove `BufferPool` and `PooledAllocator`
- Implement arena-per-subsystem for `BlockIndex`

Eliminates all known memory corruption, achieving 100% test pass rate.
```
