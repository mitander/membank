# Design

## Philosophy

KausalDB is opinionated. It's not general-purpose. Every decision optimizes for one thing: modeling code as a queryable graph for AI reasoning.

**Simplicity is the prerequisite for reliability.** We build simple systems that work correctly under hostile conditions.

## Core Principles

**Zero-Cost Abstractions**
Safety has zero runtime overhead in release builds. With 0.08µs block reads, every cycle matters.

**No Hidden Mechanics**
All control flow, memory allocation, and component lifecycles are explicit. You can trace every operation.

**Single Static Binary**
Zero dependencies. Deployment is `scp` and `./kausaldb`.

**Deterministic Testing**
We run production code against simulated disk corruption, network partitions, and power loss. Byte-for-byte reproducible.

## Architecture

### Single-Threaded Core

No data races by design. Async I/O handles concurrency. State transitions are trivial to reason about.

### Arena Memory Model

Coordinators own arenas. Submodules use coordinator interfaces. When a subsystem resets, all its memory vanishes in O(1).

```zig
pub const StorageEngine = struct {
    storage_arena: ArenaAllocator,
    coordinator: ArenaCoordinator,  // Stable interface survives arena ops

    pub fn flush_memtable(self: *StorageEngine) !void {
        // ... flush to disk ...
        self.coordinator.reset();  // All memtable memory gone
    }
};
```

### LSM-Tree Storage

Write-optimized architecture:
- Append-only WAL for durability
- In-memory memtable for recent writes
- Immutable SSTables on disk
- Background compaction without blocking writes

### Virtual File System

All I/O through VFS abstraction. Production uses real filesystem. Tests use deterministic simulation at memory speed.

## Data Model

**ContextBlock**: Atomic unit of code knowledge. 128-bit ID, source URI, JSON metadata.

**GraphEdge**: Typed relationship between blocks (`calls`, `imports`, `defines`).

This captures causal relationships, not just text similarity.

## Performance Targets

- Block operations: <50µs
- Single lookups: <10µs
- Graph traversal: <100µs for 3 hops
- WAL flush: <1ms
- Recovery: <1s per GB

These aren't aspirational. Current implementation exceeds most targets.

## Why Zig

No hidden control flow. No hidden allocations. Compile-time metaprogramming. The language philosophy aligns with ours: explicit over magical.
