# Assertion Protocol

## 1. Protocol Overview

This document specifies the two-tier assertion framework for Kausal. Its purpose is to enforce system integrity by providing robust error detection during development while maintaining explicit fail-fast behavior for unrecoverable conditions in production.

## 2. Assertion Directives

### 2.1. Debug Assertions (`assert`)

*   **Directive**: For development-time validation of logical invariants and API contracts.
*   **Runtime Behavior**: Active in debug builds. Compiled to a no-op in release builds.
*   **Authorized Use Cases**:
    *   Parameter validation (`not_null`, `positive`).
    *   State machine transition validation.
    *   API contract enforcement.

```zig
// Correct: Development-time invariant check.
assert.less_than_or_equal(self.memory_used, self.memory_limit);

// Correct: API contract validation.
assert.assert(buffer.len >= needed_size);
```

### 2.2. Fatal Assertions (`fatal_assert`)

*   **Directive**: For the detection of unrecoverable system corruption.
*   **Runtime Behavior**: Always active. Terminates the process immediately upon failure.
*   **Authorized Use Cases**:
    *   Memory safety violations (e.g., pointer aliasing from arena cloning).
    *   Systematic data corruption (e.g., 4+ consecutive WAL checksum failures).
    *   Critical invariant violations in core data structures.

```zig
// Correct: Memory accounting must never underflow. This indicates heap corruption.
fatal_assert(self.memory_used >= old_memory,
    "Memory accounting underflow: {} < {} - heap corruption",
    .{ self.memory_used, old_memory });

// Correct: A corrupted magic number indicates filesystem damage.
fatal_assert(header.magic == WAL_ENTRY_MAGIC,
    "WAL entry magic corruption: expected 0x{X}, got 0x{X}",
    .{ WAL_ENTRY_MAGIC, header.magic });
```

## 3. Error Classification Doctrine

The system's response to an error is determined by its classification.

### 3.1. Graceful Degradation (Return `error`)

*   **Doctrine**: Environmental failures and user errors are recoverable conditions. The system must handle them without terminating.
*   **Classification**:
    *   **Environmental**: File not found, permission denied, network timeouts.
    *   **Resource Exhaustion**: Out of disk space, memory pressure.
    *   **User Input**: Malformed requests, validation failures.

```zig
// Correct: Environmental failure is handled by returning an error.
pub fn find_block(self: *Engine, id: BlockId) StorageError!?ContextBlock {
    const file = self.vfs.open_file(path) catch |err| switch (err) {
        error.FileNotFound => return null,
        error.PermissionDenied => return StorageError.PermissionDenied,
        else => return StorageError.IoError,
    };
    // ...
}
```

### 3.2. Fail-Fast (Trigger `fatal_assert`)

*   **Doctrine**: Internal state corruption and safety violations are unrecoverable. The system must terminate immediately to prevent propagation of corrupted data.
*   **Classification**:
    *   **Memory Corruption**: Arena allocator failures, pointer aliasing, accounting errors.
    *   **Systematic Data Corruption**: 4+ consecutive WAL validation failures.
    *   **Critical Invariant Violations**: Data structure inconsistency, protocol violations.

## 4. Performance Specification

*   **Debug Build Overhead**: A performance reduction of 10-20% for validation is acceptable. Assertions should not be placed in tight, performance-critical loops.
*   **Release Build Overhead**: The target performance overhead from assertions is **<1%**. Debug assertions must be no-ops. Fatal assertions are acceptable as they only execute on unrecoverable error paths. Validation is performed by the benchmark regression suite in CI.
