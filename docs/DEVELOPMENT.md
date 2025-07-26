# CortexDB Development Guide

## Setup

```bash
./scripts/install_zig.sh    # Install project-specific Zig toolchain
./scripts/setup_hooks.sh    # Install git hooks (formatting, commit standards)
```

## Core Workflow

**Golden Rule**: `./zig/zig build test` must pass before every commit.

```bash
./zig/zig build test         # Build + run all tests (unit/simulation/integration)
./zig/zig build run          # Start CortexDB server
./zig/zig build check        # Quick compilation + quality checks
```

## Build Commands

```bash
# Testing
./zig/zig build simulation         # Deterministic failure scenarios
./zig/zig build storage_simulation # Storage-specific simulation tests
./zig/zig build wal_recovery       # WAL recovery validation
./zig/zig build memory_isolation   # Arena memory safety tests

# Development
./zig/zig build benchmark    # Performance benchmarks
./zig/zig build fuzz         # Fuzz testing
./zig/zig build tidy         # Code quality checks
./zig/zig build fmt          # Check code formatting
./zig/zig build fmt-fix      # Auto-fix formatting
./zig/zig build ci           # Complete CI pipeline

# Local CI
./scripts/local_ci.sh        # Run exact GitHub Actions locally
./scripts/local_ci.sh --job=test-ubuntu  # Specific job only
```

## Build Modes

**Recommended for Development**: `ReleaseSafe`

```bash
./zig/zig build test -Doptimize=ReleaseSafe  # Fast compilation + safety checks
```

**Debug Mode Issues**: Linking hangs 60+ seconds. Use `ReleaseSafe` instead - same safety checks, faster builds.

**Production**: `ReleaseFast`

## Debugging Memory Issues

**Tier 1**: Safety-enabled allocator (finds 90% of bugs instantly)

```zig
test "crashing test" {
    // Replace: const allocator = std.testing.allocator;
    var gpa = std.heap.GeneralPurposeAllocator(.{.safety = true}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // ... test code ...
}
```

**Tier 2**: Thread Sanitizer and C UBSan (detailed memory error reports)

```bash
./zig/zig build test-sanitizer
```

**Tier 3**: LLDB (only if sanitizers don't catch it)

## Project Structure

```
src/
├── cortexdb.zig              # Public API entry point
├── main.zig                  # Server binary
├── core/                     # Foundation (types, VFS, utilities)
├── storage/                  # LSM-tree coordinator + managers
│   ├── engine.zig            # StorageEngine coordinator
│   ├── memtable_manager.zig  # In-memory state
│   └── sstable_manager.zig   # On-disk state
├── query/                    # Query engine
├── ingestion/                # Data ingestion pipeline
├── server/                   # TCP server
├── sim/                      # Simulation framework
└── dev/                      # Development tools (not shippable)

tests/
├── integration/              # End-to-end workflows
├── simulation/               # Failure scenarios
├── stress/                   # High-load testing
└── recovery/                 # WAL corruption/recovery
```

## Testing Philosophy

**Don't mock, simulate.** Run real production code against simulated filesystem/network.

**VFS Pattern**: All I/O goes through `VFS` abstraction

- Production: Real filesystem
- Tests: In-memory `SimulationVFS` with fault injection

**Memory Management**: Use `std.testing.allocator` (detects leaks automatically)

## Commit Standards

**Format**: `type(scope): description`

**Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

**Example**:

```
feat(storage): add streaming WAL recovery

Reduces memory usage during startup by processing entries one at a time.

- Extract WALEntryStream for buffered I/O
- Process recovery incrementally vs loading entire segment
- Maintain identical behavior for corruption detection
- Add comprehensive recovery stress tests

Startup memory usage reduced from 200MB to <10MB for large WAL files.
```

**Rules**:

- Subject line <50 chars
- Description explains WHY, bullet points explain WHAT
- Never mention AI/Claude/assistants
- Test locally with `./zig/zig build test` first

## Code Standards

**Memory**: Explicit allocators, arena-per-subsystem pattern

```zig
// Good: Allocator parameter explicit
pub fn init(allocator: std.mem.Allocator) !MyStruct { ... }

// Bad: Hidden global allocator
pub fn init() !MyStruct { ... }
```

**Lifecycle**: Two-phase initialization for I/O components

```zig
var engine = try StorageEngine.init(allocator, vfs, data_dir);  // Phase 1: memory only
try engine.startup();  // Phase 2: I/O operations
```

**Errors**: Specific error sets, no `anyerror` in public APIs

```zig
// Good: Caller knows what to handle
const StorageError = error{BlockNotFound} || std.fs.File.ReadError;
pub fn find_block(id: BlockId) StorageError!ContextBlock { ... }

// Bad: Generic error type
pub fn find_block(id: BlockId) !ContextBlock { ... }
```

**Comments**: Explain WHY, not WHAT

```zig
// Good: Design rationale
// Linear scan faster than binary search for <16 SSTables due to cache locality
for (self.sstables.items) |sstable| { ... }

// Bad: Obvious statement
// Loop through SSTables
for (self.sstables.items) |sstable| { ... }
```

## Performance Guidelines

**Hot Path Rules**:

- No allocations during queries
- Use arenas for temporary data
- Linear scans for small collections (<16 items)
- Single-threaded core (enforced with assertions)

**Target Metrics**:

- <1ms block lookups
- <10ms graph traversals (3-hop)
- 10K writes/sec sustained

## Common Issues

**Compilation Hangs**: Switch from Debug to ReleaseSafe mode

**Memory Corruption**: Use Tier 1 debugging (safety allocator)

**Test Flakiness**: Check for cross-allocator usage, ensure consistent VFS usage

**Performance Regression**: Run benchmarks, check for allocations in hot paths

## CI/GitHub Integration

**Pre-commit Hook**: Runs `fmt`, `tidy`, and `test` automatically

**GitHub Actions**:

- Ubuntu/macOS testing
- Valgrind memory safety
- Performance regression detection
- Fuzz testing

**Local CI Mirror**: `./scripts/local_ci.sh` runs identical pipeline locally
