# Contributing

## Setup

```bash
./scripts/install_zig.sh      # Exact Zig version required
./scripts/setup_hooks.sh       # Pre-commit/push hooks (mandatory)
```

## Workflow

**Pre-commit**: Runs formatter, tidy checks, fast tests. ~10 seconds.
**Pre-push**: Full CI validation locally. ~2 minutes. Catches most CI failures.

```bash
./zig/zig build test           # Fast developer loop
./scripts/local_ci.sh          # Full CI check before push
./scripts/check_regression.sh  # Performance validation
```

## Standards

### Naming

Functions are verbs:
```zig
pub fn find_block()     // Good
pub fn block_finder()   // Bad
```

Special prefixes:
- `try_*` for error unions
- `maybe_*` for optionals
- `is_/has_/should_/can_` for booleans

### Memory

Arena coordinator pattern - coordinators own memory, submodules use interfaces:
```zig
pub const StorageEngine = struct {
    storage_arena: ArenaAllocator,
    coordinator: ArenaCoordinator,  // Stable interface
};
```

Never embed arenas in structs that get copied.

### Testing

Tests use harnesses, not manual setup:
```zig
var harness = try StorageHarness.init_and_startup(allocator, "test_db");
defer harness.deinit();
```

Manual setup requires justification:
```zig
// Manual setup required because: Recovery testing needs two separate
// StorageEngine instances sharing the same VFS
```

### Comments

Code shows WHAT. Comments explain WHY.

```zig
// BAD: Increment counter
counter += 1;

// GOOD: WAL requires sequential entry numbers for recovery validation
counter += 1;
```

## Debugging

1. **Memory issues**: Enable GPA safety
2. **Deeper analysis**: `./zig/zig build test-sanitizer`
3. **Performance**: `./zig/zig build benchmark`

## Commits

```
type(scope): brief summary

Problem and solution.

- Change 1
- Change 2

Impact: Result.
```

## Philosophy

- Correctness over features
- Explicit over magical
- Zero-cost abstractions
- Deterministic testing

Read [STYLE.md](docs/STYLE.md) for details.
