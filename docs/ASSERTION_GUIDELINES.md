# CortexDB Assertion Framework Guidelines

## Overview

CortexDB implements a **two-tier assertion framework** designed to provide robust error detection in development while maintaining explicit fail-fast behavior for unrecoverable conditions in production. This document defines when and how to use each assertion type.

## Assertion Types

### 1. Debug Assertions (`assert`)

**Purpose**: Development-time validation and debugging aids
**Runtime Behavior**: Active in debug builds, no-ops in release builds
**Use Cases**: Parameter validation, state machine transitions, API contract violations

```zig
// Good: Development validation
assert.assert(buffer.len >= needed_size);
assert.not_null(block.content);
assert.positive(payload_size);

// Good: Invariant checking during development
assert.less_than_or_equal(self.memory_used, self.memory_limit);
```

### 2. Fatal Assertions (`fatal_assert`)

**Purpose**: Unrecoverable system corruption detection
**Runtime Behavior**: Always active, terminates process immediately
**Use Cases**: Memory corruption, systematic data corruption, critical invariant violations

```zig
// Good: Memory safety violations
fatal_assert(@intFromPtr(cloned_ptr) != @intFromPtr(original_ptr),
    "Arena allocator failed to clone - heap corruption detected");

// Good: Systematic corruption detection
fatal_assert(consecutive_failures <= CORRUPTION_THRESHOLD,
    "WAL systematic corruption: {} consecutive failures", .{consecutive_failures});
```

## Error Classification Framework

### Graceful Degradation (Return Errors)

**Philosophy**: Environmental failures and user errors should be handled gracefully

**Categories**:
- **Environmental Failures**: File not found, permission denied, network timeouts
- **Resource Exhaustion**: Out of disk space, memory pressure, connection limits
- **User Input Errors**: Malformed data, validation failures, invalid requests
- **Temporary Conditions**: Lock contention, retry-able operations

```zig
// Good: Graceful error handling
pub fn find_block(self: *Engine, id: BlockId) StorageError!?ContextBlock {
    // Environmental failure - return error for caller to handle
    const file = self.vfs.open_file(path) catch |err| switch (err) {
        error.FileNotFound => return null,
        error.PermissionDenied => return StorageError.PermissionDenied,
        else => return StorageError.IoError,
    };

    // ... rest of implementation
}
```

### Fail-Fast (Fatal Assertions)

**Philosophy**: Corruption and safety violations require immediate termination

**Categories**:
- **Memory Corruption**: Arena failures, pointer aliasing, accounting errors
- **Systematic Data Corruption**: 4+ consecutive checksum/validation failures
- **Handle Corruption**: VFS handles, file descriptors, memory safety violations
- **Critical Invariant Violations**: Data structure consistency, protocol violations

```zig
// Good: Fail-fast on corruption
pub fn put_block(self: *MemtableManager, block: ContextBlock) !void {
    const old_memory = self.memory_used;
    // ... allocation logic ...

    // Critical: Memory accounting must never underflow
    fatal_assert(self.memory_used >= old_memory,
        "Memory accounting underflow: {} < {} - heap corruption",
        .{ self.memory_used, old_memory });
}
```

## Specific Guidelines by Component

### Storage Engine

**Debug Assertions**:
- Parameter validation (non-null pointers, valid ranges)
- State consistency checks (initialization flags, lifecycle)
- API contract enforcement (proper sequencing)

**Fatal Assertions**:
- Arena allocator corruption (pointer aliasing detection)
- Memory accounting violations (underflow conditions)
- VFS handle corruption (null pointers, invalid handles)

```zig
pub fn startup(self: *StorageEngine) !void {
    // Debug assertion: Lifecycle validation
    assert.assert(!self.initialized);

    // ... initialization logic ...

    // Fatal assertion: Critical handle validation
    fatal_assert(@intFromPtr(self.wal_manager) >= 0x1000,
        "WAL manager pointer corruption: 0x{X}", .{@intFromPtr(self.wal_manager)});
}
```

### WAL Processing

**Debug Assertions**:
- Entry format validation during development
- Buffer boundary checks
- Stream state consistency

**Fatal Assertions**:
- Magic number corruption (immediate filesystem damage indication)
- Systematic corruption patterns (4+ consecutive failures)
- Critical protocol violations

```zig
pub fn validate_entry_header(header: WALHeader) !void {
    // Debug assertion: Development validation
    assert.assert(header.payload_size <= MAX_PAYLOAD_SIZE);

    // Fatal assertion: Magic corruption indicates filesystem damage
    fatal_assert(header.magic == WAL_ENTRY_MAGIC,
        "WAL entry magic corruption: expected 0x{X}, got 0x{X}",
        .{ WAL_ENTRY_MAGIC, header.magic });
}
```

### Query Engine

**Debug Assertions**:
- Query parameter validation
- Result set consistency
- Performance threshold monitoring

**Fatal Assertions**:
- Graph structure corruption
- Index consistency violations
- Memory safety in traversal

## Performance Considerations

### Debug Build Overhead

**Expected Impact**: 10-20% performance reduction acceptable
- Comprehensive parameter validation
- State consistency checks
- Detailed error context

**Mitigation Strategies**:
- Use `assert.assert_fmt` only when error context is critical
- Prefer simple `assert.assert` for basic checks
- Avoid assertions in tight loops where possible

### Release Build Requirements

**Target Impact**: <1% performance overhead
- Fatal assertions only for critical corruption detection
- No-op debug assertions
- Minimal logging overhead

**Validation**:
- Benchmark regression detection in CI
- Performance tests validate assertion overhead
- Memory usage tracking for assertion impact

## Debugging and Diagnostics

### Error Context Standards

**Rich Context for Fatal Assertions**:
```zig
fatal_assert(condition,
    "Operation: {} failed with {} at position {} (expected: {}, actual: {})",
    .{ operation_name, error_type, file_position, expected_value, actual_value });
```

**Structured Logging Integration**:
```zig
// Before fatal assertion, log structured context
log.err("Corruption detected: operation={s} file={s} position={} pattern={x}",
    .{ operation, file_path, position, corruption_pattern });

fatal_assert(false, "Systematic corruption detected - see structured log");
```

### Corruption Pattern Documentation

**Memory Corruption Signatures**:
- Pointer aliasing: Same address returned for different allocations
- Accounting underflow: Memory usage decreases below previous state
- Handle corruption: Null or invalid pointers in active structures

**WAL Corruption Signatures**:
- Magic number corruption: Invalid magic indicates filesystem damage
- Systematic failures: 4+ consecutive validation failures
- Entry boundary corruption: Payload sizes exceeding reasonable limits

## Development Workflow Integration

### Pre-Commit Validation

**Required Checks**:
- All tests pass with assertions enabled
- No debug assertions in performance-critical paths
- Fatal assertions include meaningful error messages
- Error classification follows framework guidelines

### Code Review Checklist

**Assertion Usage**:
- [ ] Debug assertions used for development validation only
- [ ] Fatal assertions reserved for corruption detection
- [ ] Error conditions properly classified (graceful vs fail-fast)
- [ ] Rich context provided for fatal assertion messages

**Error Handling**:
- [ ] Environmental failures return appropriate errors
- [ ] User errors handled gracefully with clear messages
- [ ] System corruption triggers fail-fast behavior
- [ ] Temporary conditions support retry logic

## Testing Strategy

### Unit Testing

**Debug Assertion Testing**:
- Verify assertions trigger in debug builds
- Validate no-op behavior in release builds
- Test assertion message formatting

**Fatal Assertion Testing**:
- Use controlled corruption injection
- Validate immediate termination behavior
- Test diagnostic message quality

### Integration Testing

**Corruption Simulation**:
- Inject memory corruption scenarios
- Simulate systematic WAL corruption
- Test recovery behavior boundaries

**Performance Validation**:
- Measure assertion overhead in release builds
- Validate performance targets with assertions enabled
- Regression detection for assertion impact

## Production Monitoring

### Metrics Collection

**Fatal Assertion Triggers**:
- Count and categorize fatal assertion events
- Track corruption patterns and frequencies
- Monitor system health indicators

**Diagnostic Information**:
- Structured logging before termination
- Memory usage and allocation patterns
- File system integrity status

### Operational Procedures

**Fatal Assertion Response**:
1. **Immediate**: Process termination prevents data corruption
2. **Short-term**: Investigate corruption source (hardware, filesystem)
3. **Long-term**: Analyze patterns for systemic issues

**Diagnostic Data Collection**:
- Core dumps with assertion context
- System logs around termination time
- File system integrity checks
- Memory usage patterns

## Migration Guidelines

### Existing Code Updates

**Step 1**: Audit existing `std.debug.assert` usage
- Classify as development validation vs corruption detection
- Replace corruption detection with `fatal_assert`
- Add rich error context to fatal assertions

**Step 2**: Error handling classification
- Review error return patterns
- Ensure environmental failures return errors
- Convert corruption detection to fail-fast

**Step 3**: Testing integration
- Add corruption injection tests
- Validate assertion behavior in CI
- Performance regression detection

### New Feature Development

**Design Phase**:
- Define error classification boundaries
- Identify corruption detection points
- Plan assertion strategy

**Implementation Phase**:
- Use debug assertions for development validation
- Implement fatal assertions for corruption detection
- Provide rich diagnostic context

**Testing Phase**:
- Test both assertion types
- Validate performance impact
- Corruption injection testing

## Conclusion

CortexDB's assertion framework provides **explicit fail-fast behavior** for unrecoverable conditions while maintaining **graceful degradation** for environmental failures. By following these guidelines, developers ensure system reliability through early corruption detection without compromising production performance.

**Key Principles**:
- Debug assertions for development validation (no-ops in release)
- Fatal assertions for corruption detection (always active)
- Graceful error handling for environmental conditions
- Rich diagnostic context for operational debugging
- Performance-conscious implementation with <1% overhead target

The framework successfully balances **safety** (corruption detection), **performance** (minimal overhead), and **operability** (clear diagnostics) for production database systems.
