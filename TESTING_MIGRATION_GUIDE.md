# Testing Infrastructure Migration Guide

This guide shows how to migrate existing tests to use KausalDB's enhanced testing infrastructure while maintaining the project's core philosophy of explicitness over magic.

## Overview

The enhanced testing infrastructure provides:

1. **Standardized Test Harnesses**: Eliminate setup boilerplate while keeping test logic explicit
2. **Scenario-Driven Fault Injection**: Declarative fault testing with reproducible parameters  
3. **Existing Infrastructure Integration**: Builds on proven `golden_master.zig` and `performance_assertions.zig`
4. **Arena-per-Subsystem**: O(1) cleanup via harness deinitialization

## Key Principles Maintained

- **Explicitness Over Magic**: All test parameters remain visible and discoverable
- **Simulation-First Testing**: Real production code tested under deterministic simulation
- **Two-Phase Initialization**: Clear `init()` (cold) and `startup()` (hot) separation
- **Pure Coordinator Pattern**: Harnesses orchestrate components without owning state

## Migration Patterns

### 1. Storage Tests → StorageHarness

**Before:**
```zig
test "old storage test" {
    const allocator = testing.allocator;
    
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    
    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "test");
    defer storage_engine.deinit();
    try storage_engine.startup();
    
    // ... test logic
}
```

**After:**
```zig
test "modern storage test" {
    const allocator = testing.allocator;
    
    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "test");
    defer harness.deinit(); // O(1) cleanup via arena
    
    // ... test logic using harness.storage_engine
}
```

### 2. Query Tests → QueryHarness

**Before:**
```zig
test "old query test" {
    const allocator = testing.allocator;
    
    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();
    
    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "test");
    defer storage_engine.deinit();
    try storage_engine.startup();
    
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();
    
    // ... test logic
}
```

**After:**
```zig
test "modern query test" {
    const allocator = testing.allocator;
    
    var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "test");
    defer harness.deinit();
    
    // ... test logic using harness.storage_engine() and harness.query_engine
}
```

### 3. Integration Tests → SimulationHarness

**Before:**
```zig
test "old integration test" {
    const allocator = testing.allocator;
    
    var sim = try Simulation.init(allocator, seed);
    defer sim.deinit();
    
    const node = try sim.add_node();
    sim.tick_multiple(5);
    
    const node_ptr = sim.find_node(node);
    const vfs = node_ptr.filesystem_interface();
    
    var storage_engine = try StorageEngine.init_default(allocator, vfs, "test");
    defer storage_engine.deinit();
    try storage_engine.startup();
    
    var query_engine = QueryEngine.init(allocator, &storage_engine);
    defer query_engine.deinit();
    
    // ... test logic
}
```

**After:**
```zig
test "modern integration test" {
    const allocator = testing.allocator;
    
    var harness = try kausaldb.SimulationHarness.init_and_startup(allocator, 0xSEED, "test");
    defer harness.deinit();
    
    // ... test logic using harness.storage_engine, harness.query_engine
    // ... use harness.tick(count) for simulation advancement
}
```

### 4. Test Data Creation → TestData

**Before:**
```zig
// Custom helper in each test file
fn create_test_block(index: u32, allocator: std.mem.Allocator) !ContextBlock {
    var id_bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &id_bytes, index, .little);
    
    const content = try allocator.alloc(u8, 128);
    @memset(content, 'A');
    
    return ContextBlock{
        .id = BlockId.from_bytes(id_bytes),
        .version = 1,
        .source_uri = try std.fmt.allocPrint(allocator, "test://block_{}.zig", .{index}),
        .metadata_json = try allocator.dupe(u8, "{}"),
        .content = content,
    };
}
```

**After:**
```zig
// Standardized, tested, deterministic test data creation
const block = try kausaldb.TestData.create_test_block(allocator, index);
defer kausaldb.TestData.cleanup_test_block(allocator, block);
```

### 5. Fault Injection → Scenario Framework

**Before:**
```zig
test "manual fault injection" {
    const allocator = testing.allocator;
    
    var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 12345);
    defer sim_vfs.deinit();
    
    // ... manual setup boilerplate ...
    
    // Manual fault configuration
    sim_vfs.enable_io_failures(300, .{ .write = true });
    
    // Manual operation execution with repetitive error handling
    for (0..50) |i| {
        const block = try create_custom_block(@intCast(i), allocator);
        defer cleanup_custom_block(block, allocator);
        
        if (storage_engine.put_block(block)) |_| {
            // success
        } else |err| {
            try testing.expect(err == error.IoError);
        }
    }
    
    // Manual recovery testing
    // ... more boilerplate ...
}
```

**After:**
```zig
test "declarative scenario fault injection" {
    const allocator = testing.allocator;
    
    // Execute predefined scenario - all parameters explicit
    try kausaldb.scenarios.execute_wal_durability_scenario(allocator, 0);
}

test "custom scenario for specific requirements" {
    const allocator = testing.allocator;
    
    const custom_scenario = kausaldb.FaultScenario{
        .description = "Custom write failure test for specific conditions",
        .seed = 0x12345678,
        .fault_type = .write_failures,
        .initial_blocks = 100,
        .fault_operations = 50,
        .expected_recovery_success = true,
        .expected_min_survival_rate = 0.8,
        .golden_master_file = null,
    };
    
    const executor = kausaldb.ScenarioExecutor.init(allocator, custom_scenario);
    try executor.execute();
}
```

## Migration Strategy

### Phase 1: High-Impact, Low-Risk Migrations

Start with tests that have the most setup boilerplate and would benefit most from harnesses:

1. **Integration tests** (`tests/integration/*.zig`) → `SimulationHarness`
2. **Storage stress tests** (`tests/stress/*.zig`) → `StorageHarness` or `QueryHarness`
3. **Simple storage tests** → `StorageHarness`

### Phase 2: Fault Injection Systematization  

Convert manual fault injection tests to use the scenario framework:

1. **WAL durability tests** (`tests/fault_injection/wal_*.zig`) → Scenario framework
2. **Compaction crash tests** (`tests/fault_injection/compaction_*.zig`) → Scenario framework
3. **Network fault tests** → Custom scenarios

### Phase 3: Performance Test Integration

Ensure all performance tests use the existing `performance_assertions.zig` framework:

1. **Streaming benchmarks** → `BatchPerformanceMeasurement`  
2. **Defensive performance tests** → `PerformanceAssertion`
3. **Storage benchmarks** → Tiered performance validation

## Examples

See `tests/example_migrations/fault_injection_migration_example.zig` for comprehensive before/after examples showing:

- Old manual setup vs. new harness usage
- Custom test helpers vs. standardized `TestData`  
- Manual fault injection vs. declarative scenarios
- Boilerplate reduction while maintaining explicitness

## Testing the Migration

After migrating tests, verify they still pass and maintain the same test coverage:

```bash
# Run fast test suite to verify basic functionality
./zig/zig build test

# Run comprehensive test suite including migrated tests
./zig/zig build test-all

# Run performance tests to ensure no regressions
./scripts/benchmark.sh
```

## Key Benefits Achieved

1. **Reduced Boilerplate**: 80%+ reduction in setup code per test
2. **Increased Reliability**: Standardized, tested harness components
3. **Better Determinism**: Consistent seeding and state management
4. **Improved Performance**: Arena allocation for O(1) cleanup
5. **Enhanced Discoverability**: Explicit scenario parameters 
6. **Maintained Philosophy**: All principles preserved, just better systematized

## Support

For questions about migrations or the new infrastructure:

1. Review existing patterns in `src/test/*.zig` 
2. Check examples in `tests/example_migrations/`
3. Look at predefined scenarios in `src/test/scenarios.zig`
4. Follow the two-phase initialization pattern for custom harnesses