# Building Something Real

KausalDB doesn't compromise. This guide shows you how to contribute to a system that measures performance in microseconds and proves correctness through chaos.

## The Underground Way

We build differently here. No mocking. No "it works on my machine." Every line of code runs against the same hostile simulation that production faces—disk corruption, network failures, power loss. If it survives the simulator, it survives the real world.

### Reality-First Testing

The testing philosophy is simple: test reality, not approximations.

- **Virtual File System**: All storage goes through our VFS abstraction (`src/vfs.zig`). Production uses real disks. Tests use an in-memory simulation that can corrupt bytes, fail writes, and lose power at the worst possible moment.
- **Deterministic Chaos**: The simulation framework (`src/simulation.zig`) controls time, I/O, and networking with nanosecond precision. Every test failure is reproducible byte-for-byte.

When you build a feature, think like the system: *How does this break? How do I prove it won't?*

## Getting Your Hands Dirty

Clone, install, hook up. The Git hooks aren't suggestions—they're the immune system that keeps the codebase healthy.

```bash
# Get the exact Zig version this project expects
./scripts/install_zig.sh

# Install the quality gates
./scripts/setup_hooks.sh
```

## The Development Flow

### Quality Gates on Every Move

Our Git hooks run the checks that matter:

1.  **`pre-commit`**: Your code gets tested before it's committed:
    - **Formatter:** `zig fmt` - consistent style, no arguments
    - **Tidy Check:** `zig build tidy` - architectural violations get caught
    - **Fast Tests:** `zig build test` - critical paths must pass
2.  **`pre-push`**: Full local CI mirrors GitHub Actions—cross-platform builds, integration tests, performance regression checks. Most CI failures get caught here, not after you've broken main.

### The Build Arsenal

Multiple ways to test, each serving a purpose:

- **Developer Loop**: Daily verification, runs in seconds.
  ```bash
  ./zig/zig build test
  ```
- **CI Mirror**: Full suite including stress tests. Run before pushing or pay the price.
  ```bash
  ./zig/zig build ci
  ./scripts/local_ci.sh  # Script version with more options
  ```
- **Performance**: Benchmark when you're optimizing for microseconds.
  ```bash
  ./zig/zig build benchmark
  ./zig-out/bin/benchmark storage --json
  ```
- **Chaos**: Fuzz testing with different intensity levels.
  ```bash
  ./scripts/fuzz.sh quick storage  # 30 seconds of chaos
  ./scripts/fuzz.sh continuous all # Runs until you stop it
  ```

## Debugging Like a Pro

Three-tier approach: start fast, go deeper when needed.

**Tier 1: Find the Source**

Memory corruption? Enable GPA safety mode in your failing test:

```zig
var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
defer _ = gpa.deinit();
const allocator = gpa.allocator();
```

This catches buffer overflows and use-after-free at the source, not when they explode later.

**Tier 2: Nuclear Option**

Still confused? Sanitizers give you the full story:

```bash
./zig/zig build test-sanitizer
```

LLVM's AddressSanitizer provides detailed stack traces showing exactly where memory got corrupted.

**Tier 3: Interactive**

When you need to poke around manually, break out the debugger. Set breakpoints, inspect state, understand the "why" behind the crash.

## The Testing Machinery

### Battle-Tested Harnesses

No more setup boilerplate. The harnesses handle the boring stuff so you can focus on breaking things properly:

- **QueryHarness**: Query engine and storage working together
- **StorageHarness**: Pure storage testing with VFS simulation
- **NetworkHarness**: Server protocols under deterministic I/O
- **FaultInjectionHarness**: Systematic chaos—disk failures, network partitions, corruption

### How Tests Are Organized

By complexity and intent:

- **Unit Tests** (`tests/unit/`): Fast, isolated components
- **Integration Tests** (`tests/integration/`): Multiple components working together
- **Performance Tests** (`tests/performance/`): Benchmark and regression detection
- **Fault Injection** (`tests/fault_injection/`): Simulated disasters
- **Recovery Tests** (`tests/recovery/`): Data corruption and recovery scenarios

### Writing Tests That Matter

When you build something new:

1.  **Pick the right harness**: QueryHarness, StorageHarness, or NetworkHarness based on what you're testing
2.  **Test the failure modes**: Happy path is easy. Test what happens when things go wrong
3.  **Respect the memory hierarchy**: Coordinators own arenas, submodules get references, computation is pure
4.  **Isolate allocators**: Fault injection shouldn't corrupt memory across boundaries
5.  **Benchmark critical paths**: If it's performance-sensitive, prove it stays fast

### Hierarchical Memory Testing

The hierarchical memory model requires specific testing patterns to validate the coordinator→submodule→sub-submodule relationship:

**Coordinator Testing:**

```zig
test "storage coordinator memory ownership" {
    var harness = try StorageHarness.init_and_startup(allocator, "test_db");
    defer harness.deinit();

    // Coordinator owns single arena - all storage memory flows through this
    const arena_ptr = &harness.storage_engine.storage_arena;

    // Arena refresh pattern eliminates dangling references
    const coordinator = harness.storage_engine.arena_coordinator();
    // All submodules use coordinator interface, not direct arena references
}
```

### Arena Refresh Pattern

KausalDB uses an **arena refresh pattern** to eliminate dangling allocator references after arena resets:

**Pattern Design:**
- **Coordinator Interface**: Subcomponents use `ArenaCoordinator` interface instead of direct allocator references
- **Safe Allocation**: All arena allocation goes through coordinator methods that always use current arena state
- **No Temporal Coupling**: Coordinator interface remains valid after arena resets
- **Zero Runtime Overhead**: Interface dispatches through vtable with minimal cost

**Implementation Example:**
```zig
// OLD (Broken): Direct arena reference becomes invalid after reset
pub const BlockIndex = struct {
    arena_allocator: std.mem.Allocator, // Dangling after reset!
};

// NEW (Safe): Coordinator interface always uses current arena state
pub const BlockIndex = struct {
    arena_coordinator: ArenaCoordinator, // Always safe
};

// Safe allocation through coordinator
const content = try self.arena_coordinator.duplicate_u8(input_string);
```

**Benefits:**
- **Memory Safety**: Eliminates segmentation faults from dangling arena references
- **Performance**: Still enables O(1) bulk memory cleanup through coordinator
- **Simplicity**: Coordinator pattern is easier to reason about than reference management

**Allocator Isolation Testing:**

```zig
test "fault injection cannot corrupt memory hierarchy" {
    // Manual setup required because: Testing allocator isolation during fault injection
    // requires precise control over when faults are enabled relative to memory operations
    var harness = try FaultInjectionHarness.init_with_faults(allocator, 0x12345, "test_db", fault_config);
    defer harness.deinit();

    // Enable I/O failures after initialization
    harness.enable_io_failures(500, .{ .read = true });

    // Memory operations should remain stable despite I/O failures
    const result = harness.storage_engine.put_block(test_block);
    // Should succeed or fail gracefully, never corrupt memory
}
```

**Memory Leak Prevention:**

```zig
test "coordinator arena cleanup eliminates all subsystem memory" {
    var storage_engine = try StorageEngine.init(allocator, vfs, "test");

    // Add data to multiple subsystems
    try storage_engine.put_block(test_block);
    try storage_engine.put_edge(test_edge);

    // Single arena reset clears ALL storage memory through coordinator
    storage_engine.reset_storage_memory();

    // All subsystems should reflect empty state
    try testing.expectEqual(@as(u32, 0), storage_engine.memtable_manager.block_count());
}
```

### Performance Benchmarking

The benchmark tool provides manual performance validation:

- **Storage Operations**: `./zig-out/bin/benchmark storage` for block read/write performance
- **Query Operations**: `./zig-out/bin/benchmark query` for query engine performance
- **JSON Output**: Add `--json` flag for machine-readable results

## 6. Test Standardization Requirements

### Harness-First Testing Policy

All new tests MUST follow the harness-first approach unless technical requirements mandate manual setup. This policy ensures consistency, reliability, and maintainability across the test suite.

### Three-Tier Classification

**Tier 1: MUST Use Harness** (Integration/Functional Tests)

- Server lifecycle tests → `QueryHarness` or `SimulationHarness`
- Query engine integration → `QueryHarness`
- Storage + query workflows → `QueryHarness`
- Cross-component functionality → `SimulationHarness`

**Tier 2: MAY Use Manual Setup** (Specialized Tests with Justification)

- Recovery testing requiring shared VFS across engines
- Fault injection with precise timing requirements
- Performance measurement needing specific allocator control
- Memory safety testing with GPA safety features

**Tier 3: SHOULD Migrate** (Simple Unit Tests)

- Tests with >10 lines of setup boilerplate
- Tests using custom `create_*_block()` functions
- Tests manually creating `SimulationVFS` + `StorageEngine` without justification

### Required Justification Pattern

When manual setup is technically required, include explicit justification:

```zig
// Manual setup required because: [specific technical reason]
// [Additional context explaining why harness doesn't work]
var sim_vfs = try SimulationVFS.init(allocator);
defer sim_vfs.deinit();
```

### Standardized Test Data

Use `TestData.*` utilities instead of custom block creation:

```zig
// Create test blocks
const block = try TestData.create_test_block(allocator, 42);
const block_with_content = try TestData.create_test_block_with_content(allocator, 42, "content");

// Create deterministic IDs
const block_id = TestData.deterministic_block_id(42);

// Create test edges
const edge = TestData.create_test_edge_from_indices(1, 2, .calls);
```

### Automated Enforcement

The tidy system automatically enforces these patterns:

```bash
./zig/zig build tidy
```

Violations will be detected and reported:

```
Line 45: [ARCH] Consider using StorageHarness instead of manual VFS/StorageEngine setup
  Context: Manual SimulationVFS + StorageEngine pattern detected
  Suggested fix: Use StorageHarness.init_and_startup() or add justification comment
```

### CI Integration

- **Pre-commit hooks**: Automatically run tidy checks before commits
- **CI pipeline**: Fails builds with unjustified manual setup patterns
- **Quality gates**: Ensures all new tests follow standardized patterns

For detailed examples and comprehensive guidelines, see [`docs/TESTING_GUIDELINES.md`](TESTING_GUIDELINES.md).
