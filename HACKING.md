# Hacking

## Build System

```bash
./scripts/install_zig.sh    # Installs exact Zig version
./scripts/setup_hooks.sh    # Git hooks (mandatory)
./zig/zig build test        # Fast loop
./zig/zig build ci          # Full validation
```

## Testing Philosophy

Deterministic simulation. No mocks. Run production code against hostile conditions.

```zig
// All storage goes through VFS abstraction
var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0x12345);
var engine = try StorageEngine.init(allocator, sim_vfs.vfs(), "test_db");

// Inject failures deterministically
sim_vfs.enable_io_failures(500, .{ .read = true });
```

Tests are reproducible. Same seed = same failures.

## Memory Model

Arena coordinators own memory. Submodules use interfaces.

```zig
StorageEngine (owns arena) → MemtableManager (uses coordinator) → BlockIndex (pure computation)
```

Never embed arenas in copied structs. Use coordinator pattern.

## Debugging

**Tier 1: GPA Safety**
```zig
var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
defer _ = gpa.deinit();
```

**Tier 2: Sanitizers**
```bash
./zig/zig build test-sanitizer
```

**Tier 3: Interactive**
```bash
lldb ./zig-out/bin/kausaldb
```

## Performance

Benchmark first. Optimize second.

```bash
./zig/zig build benchmark
./scripts/check_regression.sh baseline  # Update baseline
./scripts/check_regression.sh           # Check for regressions
```

Current targets:
- Block write: <50µs
- Block read: <10µs
- Graph traversal: <100µs (3 hops)

## Assertions

**Debug**: Development invariants
```zig
assert.positive(block_size);
```

**Fatal**: Unrecoverable corruption
```zig
fatal_assert(self.memory_used >= size, "Memory underflow", .{});
```

## Harness Usage

Tests use harnesses. Manual setup requires justification.

```zig
// Standard pattern
var harness = try StorageHarness.init_and_startup(allocator, "test_db");
defer harness.deinit();

// Manual only with reason
// Manual setup required because: Recovery needs two engines sharing VFS
var sim_vfs = try SimulationVFS.init(allocator);
```

## Code Organization

```
src/
├── core/           # Types, assertions, memory
├── storage/        # LSM-Tree, WAL, SSTables
├── query/          # Graph traversal
├── server/         # Connection management
└── simulation/     # Test infrastructure
```

Import order: std → core → subsystems → test utilities.

## Fault Injection

Test categories:
- **Unit**: Isolated component behavior
- **Integration**: Cross-component interaction
- **Fault**: I/O failures, corruption
- **Recovery**: WAL recovery, data integrity
- **Performance**: Regression detection

## CI Pipeline

Pre-push hook runs local CI. Catches most failures.

```bash
./scripts/local_ci.sh  # ~2 minutes locally
```

GitHub Actions runs:
1. Cross-platform compilation
2. Full test suite with sanitizers
3. Performance regression detection
4. Memory leak detection

## Common Patterns

**Lifecycle**: init() → startup() → shutdown() → deinit()

**Error handling**: Explicit, no `try unreachable` without safety comment

**Naming**: Functions are verbs, `try_*` for errors, `maybe_*` for optionals
