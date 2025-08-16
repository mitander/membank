# KausalDB Logging Standards

## Overview

KausalDB uses structured, performance-conscious logging designed for production debugging and monitoring. This document defines when and how to log information consistently across all modules.

## The Two-Track System

### Track 1: User Interface (`std.debug.print`)
**Purpose**: Direct user communication - CLI output, help text, error messages
**Target**: Human operators and developers
**Lifetime**: Immediate display, not persisted

```zig
// CLI commands, help text, validation errors
std.debug.print("Usage: kausaldb server [options]\n", .{});
std.debug.print("Error: Invalid port value\n", .{});
std.debug.print("Starting KausalDB TCP server on {s}:{d}...\n", .{ host, port });
```

### Track 2: Runtime Logging (`std.log.scoped`)
**Purpose**: System behavior, debugging, monitoring
**Target**: Logs, monitoring systems, debugging
**Lifetime**: Persisted, filterable, structured

```zig
const log = std.log.scoped(.storage_engine);
log.info("Storage engine initialized: {} blocks recovered", .{block_count});
log.warn("Compaction failed: {}, retrying in {}ms", .{ err, retry_delay });
log.debug("Block read: id={} size={} latency={}ns", .{ block_id, size, latency });
```

## Runtime Logging Standards

### Module Scoping
Every module MUST use scoped logging:

```zig
const log = std.log.scoped(.module_name);
```

**Scoping Rules:**
- Use snake_case module names
- Core modules: `.storage_engine`, `.query_engine`, `.wal_recovery`
- Sub-modules: `.wal_stream`, `.tiered_compaction`, `.connection_manager`
- Tests: `.test_corruption`, `.test_lifecycle` (only for test-specific behavior)

### Log Levels and Usage

#### `log.err` - System Errors
**When**: Unrecoverable errors that require immediate attention
**Format**: `"Operation failed: {error} - context"`

```zig
log.err("WAL segment corruption: file={s} position={} checksum_mismatch", .{ file_path, position });
log.err("Storage engine startup failed: {} - data_dir={s}", .{ err, data_dir });
```

#### `log.warn` - Degraded Performance
**When**: Recoverable issues, performance degradation, retries
**Format**: `"Condition detected: description - context"`

```zig
log.warn("Compaction behind schedule: {} pending SSTables", .{pending_count});
log.warn("Connection timeout: client={s} duration={}ms", .{ client_addr, duration });
log.warn("Memory pressure: {}MB used of {}MB limit", .{ used_mb, limit_mb });
```

#### `log.info` - System State Changes
**When**: Significant state transitions, startup/shutdown, configuration
**Format**: `"State change: description"`

```zig
log.info("Server bound to {s}:{d}", .{ host, port });
log.info("Storage engine transition: {} -> {}", .{ old_state, new_state });
log.info("Compaction completed: tier={} files_merged={} duration={}ms", .{ tier, files, duration });
```

#### `log.debug` - Detailed Tracing
**When**: Fine-grained debugging information
**Compiled out**: Must be zero-cost in release builds

```zig
if (builtin.mode == .Debug) {
    log.debug("Block cache hit: id={} latency={}ns", .{ block_id, latency });
    log.debug("Query plan: {} steps, estimated_cost={}", .{ step_count, cost });
}
```

### Message Format Standards

#### Standard Format: `"action: result - context"`
```zig
// GOOD
log.info("WAL recovery completed: {} entries restored", .{entry_count});
log.warn("Connection rejected: max_connections_reached - current={} limit={}", .{ current, limit });
log.err("Block validation failed: checksum_mismatch - id={} expected=0x{X} actual=0x{X}", .{ id, expected, actual });

// BAD - Unclear action/result
log.info("Got {} things", .{count});
log.warn("Problem with stuff: {}", .{err});
```

#### Performance Metrics Format
```zig
log.info("Operation completed: duration={}ns memory={}MB throughput={:.1} ops/sec", .{ duration, memory_mb, throughput });
```

#### Error Context Format
```zig
log.err("Operation failed: {} - file={s} offset={} size={}", .{ err, file_path, offset, size });
```

## Test Logging Rules

### Test-Specific Logging
Tests should minimize logging noise. Use debug level for test-specific information:

```zig
// Test setup/teardown - debug level only
if (builtin.mode == .Debug) {
    log.debug("Test setup: creating {} test blocks", .{block_count});
}

// Expected test errors - suppress or debug level
if (builtin.mode == .Debug and err != error.TestError) {
    log.warn("Validation failed: {} - operation: {s}", .{ err, operation });
}
```

### Expected vs Unexpected Behavior
```zig
// Expected test corruption - debug level
log.debug("Test corruption injected: type={s} position={}", .{ corruption_type, position });

// Unexpected test behavior - warn level
log.warn("Test environment issue: temp_dir_cleanup_failed - path={s}", .{temp_path});
```

### Memory Leak Reporting
```zig
// Only report significant leaks or production issues
if (builtin.mode == .Debug and leak_bytes > 1024) {
    log.warn("Arena leak detected: {} objects ({} bytes) - owner={s}", .{ count, bytes, owner });
} else if (builtin.mode == .Debug) {
    log.debug("Small arena leak: {} objects ({} bytes) - owner={s}", .{ count, bytes, owner });
}
```

## Performance Requirements

### Release Build Impact
- **log.debug**: MUST be zero-cost (compiled out)
- **log.info/warn/err**: <0.1% performance impact
- **String formatting**: Avoid expensive operations in hot paths

### Debug Build Guidelines
```zig
// GOOD - Debug info is conditionally compiled
if (builtin.mode == .Debug) {
    log.debug("Hot path operation: block_id={} cache_status={s}", .{ id, cache_status });
}

// BAD - Always executes formatting
log.debug("Hot path operation: block_id={} cache_status={s}", .{ id, cache_status });
```

### Memory Allocation in Logging
- Never allocate memory for log messages in hot paths
- Use fixed-size buffers or defer expensive formatting

```zig
// GOOD - No allocation
log.info("Block written: id={} size={}", .{ block_id, size });

// BAD - Potential allocation in hot path
log.info("Block written: id={s} data={s}", .{ block_id.to_string(allocator), data });
```

## Integration Patterns

### Error Context Integration
```zig
pub fn storage_error(err: anyerror, operation: []const u8, context: StorageContext) void {
    if (builtin.mode == .Debug and err != error.TestError) {
        log.warn("Storage operation failed: {} - operation: {s}", .{ err, operation });
        if (context.file_path) |path| {
            log.warn("  file: {s}", .{path});
        }
    }
}
```

### Structured Monitoring
```zig
// Metrics logging for monitoring systems
log.info("performance_metric: operation={s} duration_ns={} memory_bytes={} throughput_ops_per_sec={:.1}",
    .{ operation_name, duration, memory, throughput });
```

### Startup/Shutdown Logging
```zig
// Clear startup sequence
log.info("KausalDB starting: version={s} data_dir={s}", .{ version, data_dir });
log.info("Storage engine initialized: {} blocks recovered", .{block_count});
log.info("Query engine ready: max_connections={}", .{max_connections});
log.info("Server listening: {s}:{d}", .{ host, port });

// Clean shutdown sequence
log.info("Graceful shutdown initiated");
log.info("Storage engine stopped: {} blocks flushed", .{block_count});
log.info("KausalDB shutdown complete");
```

## Anti-Patterns

### Don't Do This
```zig
// BAD - No context
log.info("Done");

// BAD - Byte arrays instead of meaningful data
log.warn("Block transfer: {any} -> {any}", .{ from_bytes, to_bytes });

// BAD - std.debug.print for runtime logging
std.debug.print("Storage engine started\n", .{});

// BAD - log.warn for CLI output
log.warn("Usage: kausaldb server [options]");

// BAD - Always-on debug formatting in hot paths
log.debug("Block read: {s}", .{ expensive_debug_format(block) });
```

### Do This Instead
```zig
// GOOD - Clear context
log.info("WAL recovery completed: {} entries processed", .{entry_count});

// GOOD - Meaningful identifiers
log.warn("Ownership transfer: block_id={} from={s} to={s}", .{ block_id, from_owner, to_owner });

// GOOD - Appropriate logging track
std.debug.print("Usage: kausaldb server [options]\n", .{});
log.info("Storage engine initialized");

// GOOD - Conditional debug formatting
if (builtin.mode == .Debug) {
    log.debug("Block read: {s}", .{ expensive_debug_format(block) });
}
```

## Enforcement

### Code Review Checklist
- [ ] Uses appropriate logging track (std.debug.print vs std.log)
- [ ] Follows scoped logging pattern with correct module name
- [ ] Uses appropriate log level (debug/info/warn/err)
- [ ] Follows standard message format
- [ ] Debug logging is conditionally compiled
- [ ] No expensive operations in log formatting
- [ ] Test logging minimizes noise

### Automated Validation
The CI pipeline checks for:
- Proper scoped logging usage
- No std.debug.print in runtime code paths
- No log.warn/err for CLI output
- Debug logging performance patterns

This logging standard ensures consistent, performant, and maintainable logging across the entire KausalDB codebase.
