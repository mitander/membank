# Membank Development Guide

## Setup

```bash
./scripts/install_zig.sh    # Install project-specific Zig toolchain
./scripts/setup_hooks.sh    # Install git hooks (formatting, commit standards)
```

## Core Workflow

**Golden Rule**: `./zig/zig build test` (fast unit tests) must pass before every commit. Use `test-fast` for pre-commit validation.

```bash
./zig/zig build test         # Fast unit tests (< 30 seconds, developer default)
./zig/zig build test-fast    # Comprehensive tests (~2 minutes, CI validation)
./zig/zig build test-all     # All tests including stress tests (~5 minutes, full validation)
./zig/zig build run          # Start Membank server
./zig/zig build check        # Quick compilation + quality checks
```

## Build Commands

```bash
# Testing
./zig/zig build simulation         # Deterministic failure scenarios
./zig/zig build storage_simulation # Storage-specific simulation tests
./zig/zig build wal_recovery       # WAL recovery validation
./zig/zig build memory_isolation   # Arena memory safety tests

# Developmen
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
├── membank.zig              # Public API entry poin
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
- Process recovery incrementally vs loading entire segmen
- Maintain identical behavior for corruption detection
- Add comprehensive recovery stress tests

Startup memory usage reduced from 200MB to <10MB for large WAL files.
```

**Rules**:

- Subject line <50 chars
- Description explains WHY, bullet points explain WHAT
- Never mention AI/Claude/assistants
- Test locally with `./zig/zig build test` first, then `./zig/zig build test-fast` for comprehensive validation

## Code Standards

**Tidy Suppression Mechanism**: Override quality checks with explicit justifications

Membank's tidy system enforces architectural constraints but provides escape hatches for legitimate cases. Use suppression comments sparingly and with clear justification.

```zig
// Performance suppressions - avoid false positives in hot paths
fn process_blocks() !void {
    var blocks = std.ArrayList(Block).init(allocator);
    try blocks.ensureTotalCapacity(1000); // tidy:ignore-perf - capacity managed explicitly

    for (input_data) |data| {
        try blocks.append(process_data(data)); // No violation - capacity pre-allocated
    }
}

// Architecture suppressions - legitimate threading abstractions
pub const ThreadSafeCounter = struct {
    inner: std.Thread.Mutex = .{}, // tidy:ignore-arch - safe abstraction over threading primitives
    count: u64 = 0,
};

// Length suppressions - complex but necessary function signatures
pub fn create_storage_engine( // tidy:ignore-length - complex initialization requires many parameters
    allocator: std.mem.Allocator,
    vfs: *VirtualFileSystem,
    config: StorageConfig,
    metrics: *MetricsCollector,
) !StorageEngine {
    // ...
}

// Pattern detection exceptions - tidy system self-reference
fn check_thread_usage(source: []const u8) ?[]const u8 {
    if (mem.indexOf(u8, source, "std.Thread") != null) { // tidy:ignore-arch - pattern detection for architecture compliance
        return "avoid raw threading";
    }
}
```

**Suppression Types**:
- `// tidy:ignore-perf` - Performance false positives (capacity management, test code)
- `// tidy:ignore-arch` - Architecture violations (safe abstractions, pattern detection)
- `// tidy:ignore-length` - Function declaration length (complex but necessary signatures)
- `// tidy:ignore-error` - Error handling patterns (self-referential code detection)
- `// tidy:ignore-naming` - Naming conventions (specialized cases, external APIs)
- `// tidy:ignore-generic` - Generic constraints (complex type relationships)

**Usage Guidelines**:
1. **Justify every suppression** - Comment must explain WHY the violation is acceptable
2. **Be specific** - Use targeted suppressions, not blanket exceptions
3. **Review regularly** - Suppressions indicate technical debt or false positives
4. **Prefer fixes** - Only suppress when architectural constraints require it

**Memory**: Explicit allocators, arena-per-subsystem pattern

```zig
// Good: Allocator parameter explici
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

// Bad: Obvious statemen
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
