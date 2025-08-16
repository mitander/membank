# Assertion Protocol for Production Systems

## Overview

This document defines KausalDB's two-tier assertion framework designed for microsecond-scale performance with bulletproof correctness. The system distinguishes between **programming errors** (fatal) and **runtime conditions** (recoverable) to maximize both safety and availability.

## Core Philosophy

**Fail-fast on corruption, degrade gracefully on environment.**

- **Corruption = Immediate termination**: Memory corruption, invariant violations, and systematic data corruption indicate compromised system integrity
- **Environment = Error propagation**: Network failures, disk full, permission denied are recoverable conditions that must not crash the system

## The Two-Tier System

### Tier 1: Debug Assertions (`assert`, `assert_fmt`)

**Purpose**: Development-time contract validation
**Runtime Behavior**: Active in debug builds, compiled to no-ops in release builds
**Performance Impact**: 10-20% overhead acceptable in debug mode

**Authorized Use Cases**:
```zig
// API contract validation
assert(block_id.is_valid());
assert_fmt(offset < buffer.len, "Offset {} exceeds buffer {}", .{ offset, buffer.len });

// State machine preconditions
assert(self.state.can_write());

// Algorithm invariants during development
assert(sorted_array_is_sorted(array));
```

### Tier 2: Fatal Assertions (`fatal_assert`)

**Purpose**: Detection of unrecoverable system corruption
**Runtime Behavior**: Always active, immediate process termination
**Performance Impact**: <1% overhead (corruption paths only)

**Authorized Use Cases ONLY**:

#### Memory Safety Violations
```zig
// Arena allocator corruption detection
fatal_assert(arena.bytes_allocated >= old_bytes,
    "Arena memory accounting corruption: {} < {}",
    .{ arena.bytes_allocated, old_bytes });

// Pointer arithmetic bounds checking
fatal_assert(@intFromPtr(end) >= @intFromPtr(start),
    "Invalid pointer arithmetic: end < start");
```

#### Systematic Data Corruption
```zig
// Multiple consecutive validation failures indicate filesystem corruption
if (consecutive_checksum_failures >= 4) {
    fatal_assert(false, "Systematic WAL corruption: {} consecutive failures",
        .{consecutive_checksum_failures});
}

// Core data structure magic number corruption
fatal_assert(header.magic == SSTABLE_MAGIC,
    "SSTable magic corruption: expected 0x{X}, got 0x{X}",
    .{ SSTABLE_MAGIC, header.magic });
```

#### Critical Invariant Violations
```zig
// Block index consistency
fatal_assert(block_count <= capacity,
    "Block index overflow: {} blocks > {} capacity",
    .{ block_count, capacity });
```

## Error Classification Decision Tree

Use this decision tree to determine the correct error handling approach:

```
Is this a programming error that should never happen in correct code?
├─ YES: Use debug assertion (assert/assert_fmt)
│
Is this a runtime condition that could happen in normal operation?
├─ YES: Return appropriate error type
│   ├─ File not found → return StorageError.BlockNotFound
│   ├─ Permission denied → return VFSError.AccessDenied
│   ├─ Out of disk space → return VFSError.NoSpaceLeft
│   └─ Invalid user input → return QueryError.InvalidQuery
│
Does this indicate memory corruption or systematic data corruption?
├─ YES: Use fatal_assert (immediate termination)
│   ├─ Arena accounting errors
│   ├─ 4+ consecutive checksum failures
│   └─ Core data structure corruption
│
└─ UNCLEAR: Default to returning an error (fail-safe principle)
```

## Common Misuse Patterns (DO NOT DO)

### ❌ Using fatal_assert for API Validation
```zig
// WRONG: This should return an error
pub fn write_data(data: []const u8) !void {
    fatal_assert(data.len > 0, "Cannot write empty data", .{});  // BAD
}

// CORRECT: Return error for invalid input
pub fn write_data(data: []const u8) !void {
    if (data.len == 0) return error.EmptyData;  // GOOD
}
```

### ❌ Using fatal_assert for State Validation
```zig
// WRONG: File access mode is runtime state
pub fn write_to_file(self: *File, data: []const u8) !void {
    fatal_assert(self.access_mode.can_write(), "File opened read-only", .{});  // BAD
}

// CORRECT: Check state and return appropriate error
pub fn write_to_file(self: *File, data: []const u8) !void {
    if (!self.access_mode.can_write()) return error.ReadOnlyFile;  // GOOD
}
```

### ❌ Using fatal_assert for Resource Exhaustion
```zig
// WRONG: Disk space is an environmental condition
fatal_assert(available_space > needed_space, "Insufficient disk space", .{});  // BAD

// CORRECT: Handle resource exhaustion gracefully
if (available_space < needed_space) return error.InsufficientSpace;  // GOOD
```

## Integration with Error Types

KausalDB defines specific error types for different subsystems. Always use the appropriate error type rather than fatal assertions:

```zig
// Storage errors
StorageError.BlockNotFound, StorageError.NotInitialized

// VFS errors
VFSError.FileNotFound, VFSError.AccessDenied, VFSError.OutOfMemory

// Query errors
QueryError.EmptyQuery, QueryError.TooManyResults, QueryError.InvalidSemanticQuery

// WAL errors
WALError.CorruptedEntry, WALError.InvalidOffset, WALError.IoError
```

## Performance Requirements

### Release Build Performance
- **Target**: <1% overhead from fatal assertions
- **Validation**: Benchmark regression suite in CI must pass
- **Hot paths**: No assertions in microsecond-critical loops

### Debug Build Performance
- **Target**: 10-20% overhead acceptable for validation
- **Coverage**: Comprehensive contract checking enabled
- **Testing**: All assertions must be exercised by test suite

## Memory Corruption Detection Patterns

### Arena Allocator Safety
```zig
pub fn allocate_block(arena: *Arena, size: usize) ![]u8 {
    const old_bytes = arena.bytes_allocated;
    const result = try arena.allocator().alloc(u8, size);

    // Detect arena accounting corruption
    fatal_assert(arena.bytes_allocated >= old_bytes,
        "Arena memory corruption: allocated {} < previous {}",
        .{ arena.bytes_allocated, old_bytes });

    return result;
}
```

### Pointer Arithmetic Validation
```zig
pub fn validate_slice_bounds(buffer: []const u8, offset: usize, size: usize) ![]const u8 {
    // Check for arithmetic overflow (corruption detection)
    const end_offset = std.math.add(usize, offset, size) catch {
        fatal_assert(false, "Arithmetic overflow in bounds: {} + {}", .{ offset, size });
        unreachable;
    };

    // Bounds checking (may be invalid input, not corruption)
    if (end_offset > buffer.len) return error.BoundsExceeded;

    return buffer[offset..end_offset];
}
```

### Data Structure Integrity
```zig
pub fn validate_block_index(index: *BlockIndex) void {
    // Critical invariant - violation indicates memory corruption
    fatal_assert(index.count <= index.capacity,
        "BlockIndex corruption: count {} > capacity {}",
        .{ index.count, index.capacity });

    // Verify magic number hasn't been corrupted
    fatal_assert(index.magic == BLOCK_INDEX_MAGIC,
        "BlockIndex magic corruption: expected 0x{X}, got 0x{X}",
        .{ BLOCK_INDEX_MAGIC, index.magic });
}
```

## Testing Requirements

### Assertion Coverage
- Every fatal assertion must be covered by a test case
- Debug assertions must be validated in development builds
- Error path testing must verify proper error propagation

### Simulation Testing
```zig
test "memory corruption detection" {
    // Verify fatal assertions trigger on actual corruption
    var corrupted_arena = test_arena;
    corrupted_arena.bytes_allocated = std.math.maxInt(usize); // Simulate corruption

    // This should trigger fatal_assert and terminate
    const result = std.testing.expectError(error.TestExpectedPanic,
        corrupted_arena.allocate(100));
}

test "graceful error handling" {
    // Verify environmental errors return proper error codes
    const result = try_open_missing_file("nonexistent.dat");
    try testing.expectError(StorageError.FileNotFound, result);
}
```

## Implementation Guidelines

### Fatal Assertion Format
```zig
fatal_assert(condition, "Context: expected vs actual format", .{ expected, actual });
```

### Error Context Integration
```zig
// Provide rich error context for debugging
pub fn read_block(self: *Storage, id: BlockId) StorageError!ContextBlock {
    const block = self.find_block(id) orelse {
        return error_context.storage_error(
            error.BlockNotFound,
            "read_block",
            "Block {} not found in storage engine",
            .{id}
        );
    };
    return block;
}
```

### State Machine Validation
```zig
// Use appropriate assertions for state validation
pub fn transition_state(self: *Component, new_state: State) !void {
    // Programming error - should never happen with correct usage
    if (builtin.mode == .Debug) {
        assert(self.state.can_transition_to(new_state));
    }

    // Runtime validation for external state changes
    if (!self.state.can_transition_to(new_state)) {
        return error.InvalidStateTransition;
    }

    self.state = new_state;
}
```

## Enforcement and Code Review

### Automatic Validation
- CI pipeline checks for assertion usage patterns
- Code review checklist includes assertion appropriateness
- Static analysis flags potential misuse

### Review Criteria
1. **Fatal assertions**: Only for corruption detection
2. **Debug assertions**: Only for contract validation
3. **Error returns**: All environmental and user input conditions
4. **Performance impact**: Measured and within limits
5. **Test coverage**: All assertion paths covered

This protocol ensures KausalDB maintains both microsecond performance and bulletproof reliability by applying the right tool for each type of condition.
