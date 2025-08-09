# Style Guide

This document defines the code style, naming conventions, and patterns used in the project. These are not merely suggestions; they are a set of rules that enforce our architectural principles, improve readability, and eliminate bugs. Adherence is mandatory and checked by the `pre-commit` hook.

## 1. Naming Conventions

- **Functions & Variables:** `snake_case`.
- **Constants:** `SCREAMING_SNAKE_CASE`.
- **Types & Enums:** `PascalCase`.
- **Generic Functions:** Functions that return a `type` must be `PascalCase` (e.g., `bit_set_type`).
- **Avoid `get_` and `set_` prefixes.** Use descriptive verbs (`find_block`, `compact_sstables`) or direct field access. Simple getters and setters add noise and are discouraged.

## 2. Comment Philosophy: Explain "Why", Not "What"

Comments must justify their existence by explaining non-obvious rationale, design decisions, or critical constraints. We assume a competent developer is reading the code. **Never explain what the code is doing if the code itself is clear.**

### Forbidden Comment Patterns

- **Restating the obvious:**
  ```zig
  // BAD: Loop through all items
  for (items) |item| { ... }
  ```
- **Commented-out code:** Delete it. Git has the history.
- **Development noise:** `// TODO`, `// FIXME`, `// HACK`. Resolve these before committing. Our tidy checker bans them.

### Required Comment Patterns

- **Design Rationale & Trade-offs:** Explain _why_ a particular implementation was chosen over alternatives.
  ```zig
  // GOOD: We use linear scan instead of binary search here. For the expected
  // number of SSTables (< 16), the simpler code and better cache locality
  // outweigh the O(log n) algorithmic benefit.
  for (self.index.items) |entry| { ... }
  ```
- **Safety Guarantees:** When using `unreachable` or unsafe operations, a `// Safety:` comment is mandatory.
  ```zig
  // Safety: The file handle is guaranteed to be valid by the VFS lifecycle,
  // which prevents use-after-free scenarios.
  const file = vfs.open(path) catch unreachable;
  ```
- **API Documentation:** All public functions must have `///` doc comments explaining their purpose, parameters, and error conditions.

## 3. Memory Management: Arena-per-Subsystem

- **Stateful Components:** Any component that manages a collection of objects with a shared, clear lifecycle (e.g., a memtable, a connection's state) **must** use an `ArenaAllocator`.
- **O(1) Cleanup:** The arena enables all memory for that subsystem to be freed in a single `reset` or `deinit` call. This is mandatory for performance and safety.
- **No Cross-Contamination:** Data must be explicitly cloned into an arena. An arena should never store direct pointers to memory owned by another allocator. Fatal assertions in debug builds enforce this.

## 4. Error Handling: Explicit and Contextual

- **No `try unreachable`:** This pattern is banned. Handle errors explicitly or use `catch unreachable` with a mandatory `// Safety:` comment explaining why the error is impossible.
- **Systematic Error Context:** Public API boundaries **must** wrap errors with contextual information using the helpers in `src/core/error_context.zig`. This provides rich, structured information for debugging failures.

## 5. Architectural Patterns

- **Two-Phase Initialization:** Any component that performs I/O **must** follow a two-phase `init()` / `startup()` pattern.
  - `init()`: COLD. Memory allocation only. Must not perform any I/O.
  - `startup()`: HOT. Performs all I/O operations (opening files, binding sockets, etc.).
- **Lifecycle Naming:**
  - `startup()`: The public function for Phase 2 initialization.
  - `run()`: Reserved for private, blocking event loops.

## 6. Testing Infrastructure Patterns

### Test Organization

Tests follow a four-tier hierarchy based on complexity and purpose:

- **Unit Tests** (`tests/unit/`): Fast, isolated component tests
- **Integration Tests** (`tests/integration/`): Cross-component interaction validation
- **Performance Tests** (`tests/performance/`): Benchmark and regression detection
- **Fault Injection** (`tests/fault_injection/`): Hostile condition simulation

### Test Harnesses

All tests must use the appropriate harness for coordinated setup and teardown:

- **QueryHarness**: Query engine and storage engine coordination
- **StorageHarness**: Storage subsystem testing with VFS integration
- **NetworkHarness**: Server and protocol testing with deterministic I/O
- **FaultInjectionHarness**: Systematic failure simulation and recovery testing

### Test Patterns

- **Proper Resource Management**: Tests must properly clean up resources using appropriate allocators
- **VFS Integration**: All I/O operations must use the VFS abstraction for deterministic testing
- **Failure Mode Coverage**: Every success path requires corresponding failure mode tests
- **Performance Integration**: Performance-critical paths must include benchmark coverage

### Test Comments

Tests are the only exception to the "why not what" comment rule. Test scenarios may use descriptive "what" comments to create readable narratives:

```zig
test "network partition recovery" {
    // Create three-node cluster
    var cluster = try TestCluster.init(allocator, 3);

    // Allow cluster to stabilize
    try cluster.tick_until_stable();

    // Partition node 3 from nodes 1 and 2
    cluster.partition_nodes(.{1, 2}, .{3});

    // Verify leader election continues in majority partition
    try testing.expect(cluster.has_leader(.{1, 2}));
}
```

## 7. Commit Messages

Commit messages must follow the [Conventional Commits](https://www.conventionalcommits.org/) format. The `commit-msg` hook enforces this.

- **Format:** `type(scope): brief summary`
- **Concise Body:** A brief (1-2 sentence) description, followed by bullet points detailing the changes.
- **Impact:** A final, single sentence stating the result.
- **CRITICAL:** Never mention AI or assistants in commit messages. All output must appear as if written by a senior human developer.

**Example:**

```
fix(storage): resolve HashMap corruption via arena allocator

Memory corruption from mixed PooledAllocator and backing allocator usage
was causing sporadic crashes after ~15 test cycles.

- Remove `BufferPool` and `PooledAllocator` entirely.
- Implement arena-per-subsystem memory model for `BlockIndex`.
- Add `BlockIndex.clear()` for efficient O(1) memory reset.
```
