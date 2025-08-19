# Testing Architecture

Deterministic simulation. Production code runs against hostile conditions.

## Virtual File System

All I/O through VFS abstraction:
```zig
pub const VFS = union(enum) {
    real: RealVFS,        // Production
    simulation: SimVFS,   // Testing
};
```

## Simulation Framework

Control time, I/O, and failures deterministically:
```zig
var sim = try Simulation.init(allocator, 0x12345);  // Seed = reproducible
sim.inject_io_failure_at(500);                      // Fail at operation 500
sim.corrupt_block_at(1024);                         // Corrupt specific block
```

## Test Harnesses

```zig
StorageHarness      // Storage-only tests
QueryHarness        // Storage + query tests
SimulationHarness   // Multi-node simulation
FaultHarness        // Systematic fault injection
```

## Fault Injection

Deterministic failure modes:
- I/O errors at specific operations
- Checksum corruption patterns
- Network partition simulation
- Power loss scenarios

## Assertions

```zig
assert.positive(x)           // Debug only
fatal_assert(x > 0, "msg")   // Always active
```

## Coverage

- 500+ unit tests
- 100+ integration tests
- 50+ fault injection scenarios
- Continuous fuzzing

## Performance

Test overhead acceptable for CI:
- Local: 5x multiplier (250µs)
- CI: 20x multiplier (1000µs)
- Production: 1x (50µs target)
