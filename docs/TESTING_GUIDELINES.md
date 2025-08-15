# Testing Reality, Not Stories

Testing in KausalDB isn't about checking boxes—it's about proving your code works when everything breaks. Our harnesses eliminate the boilerplate so you can focus on the chaos.

## Harnesses Over Setup Hell

Why write 20 lines of setup when you can write 2? Our harness system handles the coordination dance of components, memory management, and cleanup automatically.

- **Consistency**: Every test starts from the same foundation
- **Reliability**: Arena-per-subsystem prevents leaks, even when tests fail
- **Simplicity**: Complex component initialization becomes one function call
- **Focus**: Spend time testing edge cases, not setting up basic infrastructure

Harnesses provide coordinated component lifecycle with automatic cleanup. Write the test, not the setup.

## Three-Tier Classification System

### Tier 1: MUST Use Harness API (Integration/Functional Tests)

**Rule**: Tests that validate component interactions MUST use standardized harnesses.

**Rationale**: Integration tests benefit most from coordinated component setup and should demonstrate best practices for the codebase.

**Examples**:
- Server lifecycle tests → `QueryHarness` or `SimulationHarness`
- Query engine integration → `QueryHarness`
- Storage + query workflows → `QueryHarness`
- Cross-component functionality → `SimulationHarness`

**Harness Selection**:
```zig
// Storage-only tests
var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "test_db");
defer harness.deinit();

// Storage + Query tests
var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "test_db");
defer harness.deinit();

// Multi-node simulation tests
var harness = try kausaldb.SimulationHarness.init_and_startup(allocator, 0x12345, "test_db");
defer harness.deinit();

// Fault injection tests
var fault_config = FaultInjectionConfig{};
fault_config.io_failures.enabled = true;
var harness = try kausaldb.FaultInjectionHarness.init_with_faults(allocator, 0x12345, "test_db", fault_config);
defer harness.deinit();
```

### Tier 2: MAY Use Manual Setup (Specialized Tests)

**Rule**: Tests with legitimate technical requirements for manual setup are permitted with proper justification.

**Justification Required**: Every manual setup MUST include a comment explaining why harness usage is inappropriate.

**Comment Pattern**:
```zig
// Manual setup required because: [specific technical reason]
// [Additional context explaining why harness doesn't work]
var sim_vfs = try SimulationVFS.init(allocator);
defer sim_vfs.deinit();
```

**Legitimate Cases**:

**Recovery Testing**:
```zig
// Manual setup required because: Recovery testing needs two separate
// StorageEngine instances sharing the same VFS to validate WAL recovery
// across engine lifecycle. StorageHarness is designed for single-engine scenarios.
var sim_vfs = try SimulationVFS.init(allocator);
defer sim_vfs.deinit();

// First engine writes data
var storage_engine1 = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "test_db");
defer storage_engine1.deinit();
// ... write data, close engine ...

// Second engine recovers data
var storage_engine2 = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "test_db");
defer storage_engine2.deinit();
```

**Precise Fault Injection Timing**:
```zig
// Manual setup required because: Test needs to insert data first, then enable
// I/O failures to simulate corruption during reads. FaultInjectionHarness
// applies faults during startup(), which would prevent initial data insertion.
var sim_vfs = try SimulationVFS.init_with_fault_seed(allocator, 0xABCDE);
defer sim_vfs.deinit();

// Setup storage and insert test data
var engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "test_db");
defer engine.deinit();
try engine.startup();
try engine.put_block(test_block);

// NOW enable I/O failures for testing
sim_vfs.enable_io_failures(500, .{ .read = true });
```

**Performance Measurement**:
```zig
// Manual setup required because: Performance tests need specific allocator
// configurations for measurement isolation. Harness arena allocation would
// interfere with accurate memory usage tracking.
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
defer _ = gpa.deinit();
const allocator = gpa.allocator();
```

**Memory Safety Testing**:
```zig
// Manual setup required because: Memory safety validation requires
// GeneralPurposeAllocator with safety features enabled to detect
// buffer overflows and use-after-free errors.
var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
defer _ = gpa.deinit();
const allocator = gpa.allocator();
```

### Tier 3: SHOULD Migrate (Simple Unit Tests)

**Rule**: Simple tests using manual setup SHOULD be migrated to harnesses unless there's a specific technical reason.

**Migration Targets**:
- Tests with >10 lines of setup boilerplate
- Tests using custom `create_*_block()` functions
- Tests manually creating `SimulationVFS` + `StorageEngine` without justification

**Before (Bad)**:
```zig
test "block storage basic operations" {
    const allocator = testing.allocator;

    var sim_vfs = try SimulationVFS.init(allocator);
    defer sim_vfs.deinit();

    var storage_engine = try StorageEngine.init_default(allocator, sim_vfs.vfs(), "test_db");
    defer storage_engine.deinit();
    try storage_engine.startup();

    // Create custom test block
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
    // ... rest of test
}
```

**After (Good)**:
```zig
test "block storage basic operations" {
    const allocator = testing.allocator;

    var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "test_db");
    defer harness.deinit();

    // Use standardized test data
    const content = try std.fmt.allocPrint(allocator, "test content {}", .{42});
    defer allocator.free(content);
    const block = try TestData.create_test_block_with_content(allocator, 42, content);
    defer {
        allocator.free(block.source_uri);
        allocator.free(block.metadata_json);
        allocator.free(block.content);
    }

    try harness.storage_engine.put_block(block);
    // ... rest of test
}
```

## Standardized Test Data Utilities

Use `TestData.*` utilities instead of custom block creation functions:

### Block Creation
```zig
// Simple test block with deterministic ID
const block = try TestData.create_test_block(allocator, 42);

// Test block with custom content
const block = try TestData.create_test_block_with_content(allocator, 42, "custom content");

// Deterministic block ID for consistent testing
const block_id = TestData.deterministic_block_id(42);
```

### Edge Creation
```zig
// Create edge between blocks using indices
const edge = TestData.create_test_edge_from_indices(1, 2, .calls);

// Create edge with specific block IDs
const edge = TestData.create_test_edge(source_id, target_id, .imports);
```

## Harness API Reference

### StorageHarness
**Use for**: Storage engine testing without query operations

```zig
var harness = try kausaldb.StorageHarness.init_and_startup(allocator, "db_name");
defer harness.deinit();

// Access components
harness.storage_engine.put_block(block);
harness.vfs().create("test_file");  // Direct VFS access
```

### QueryHarness
**Use for**: Tests requiring both storage and query engines

```zig
var harness = try kausaldb.QueryHarness.init_and_startup(allocator, "db_name");
defer harness.deinit();

// Access components
harness.storage_engine.put_block(block);
harness.query_engine.find_block(block_id);
```

### SimulationHarness
**Use for**: Multi-node distributed system testing

```zig
var harness = try kausaldb.SimulationHarness.init_and_startup(allocator, 0x12345, "db_name");
defer harness.deinit();

// Access simulation components
harness.simulation.tick();
harness.node().filesystem.create("test_file");
```

### FaultInjectionHarness
**Use for**: Systematic fault injection testing

```zig
var fault_config = FaultInjectionConfig{};
fault_config.io_failures.enabled = true;
fault_config.io_failures.failure_rate_per_thousand = 100;  // 10%
fault_config.io_failures.operations.read = true;

var harness = try kausaldb.FaultInjectionHarness.init_with_faults(
    allocator, 0x12345, "db_name", fault_config
);
defer harness.deinit();
```

## Automated Enforcement

### Tidy Rule: `test_harness_usage`

The tidy system automatically detects manual setup patterns and suggests harness usage:

```bash
./zig/zig build tidy
```

**Detected Pattern**:
```
Line 45: [ARCH] Consider using StorageHarness instead of manual VFS/StorageEngine setup
  Context: Manual SimulationVFS + StorageEngine pattern detected
  Suggested fix: Use StorageHarness.init_and_startup() or add justification comment
```

**Suppression**: Add justification comment to skip tidy enforcement:
```zig
// Manual setup required because: [specific reason]
var sim_vfs = try SimulationVFS.init(allocator);
```

### CI Integration

The tidy rule runs automatically in CI and will fail builds with unjustified manual setup patterns. This ensures:

- New tests follow standardized patterns
- Manual setup requires explicit justification
- Test quality remains consistent across the codebase

## Migration Strategy

When migrating existing tests to harnesses:

1. **Identify the test type**: Storage-only, query, or simulation needs
2. **Choose appropriate harness**: StorageHarness, QueryHarness, or SimulationHarness
3. **Replace manual setup**: Swap VFS/StorageEngine creation with harness init
4. **Update component access**: Use `harness.storage_engine` instead of local variable
5. **Verify tests pass**: Run `./zig/zig build test` to ensure functionality preserved
6. **Check tidy compliance**: Run `./zig/zig build tidy` to verify no violations

## Benefits of Harness Usage

### Memory Safety
- **Arena-per-subsystem**: O(1) cleanup prevents memory leaks
- **Coordinated lifecycle**: Components properly initialized and shutdown
- **Resource isolation**: Test failures don't leak resources

### Test Reliability
- **Consistent setup**: Identical initialization across tests
- **Reduced boilerplate**: Complex setup logic centralized
- **Error prevention**: Harnesses handle edge cases in initialization

### Developer Experience
- **Faster test writing**: Standard patterns reduce cognitive load
- **Easier debugging**: Consistent setup simplifies issue investigation
- **Clear examples**: Harness usage demonstrates best practices

## Summary

The harness system provides a foundation for reliable, maintainable testing in KausalDB. By following these guidelines, tests become:

- **Consistent**: All tests use standardized patterns
- **Reliable**: Arena-based memory management prevents leaks
- **Maintainable**: Centralized setup logic benefits all tests
- **Enforceable**: Tidy rules ensure compliance automatically

When in doubt, prefer harness usage over manual setup. When manual setup is required, document the rationale clearly.
