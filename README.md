# CortexDB

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI Status](https://github.com/mitander/cortexdb/actions/workflows/ci.yml/badge.svg)](https://github.com/mitander/cortexdb/actions)

**Fast, deterministic context database for Large Language Models.**

CortexDB eliminates LLM context drift by providing millisecond access to structured, interconnected knowledge. Built in Zig with an obsessive focus on correctness and performance.

## Why CortexDB?

LLMs operate on tiny, transient context windows. CortexDB stores **structured knowledge graphs** instead of flat text:

- **Context Blocks**: Logical chunks (functions, paragraphs, configs) with metadata and versioning
- **Typed Relationships**: `IMPORTS`, `CALLS`, `REFERENCES` edges enable graph traversal
- **Sub-millisecond Queries**: LSM-tree storage optimized for read-heavy LLM workloads

## Architecture

Built on battle-tested principles from high-frequency trading systems:

- **Explicit Everything**: No hidden allocations, no magic, no surprises
- **Single-threaded Core**: Zero data races by design
- **Arena Memory Model**: Bulk allocation/deallocation, eliminates entire bug classes
- **Simulation-first Testing**: Deterministic reproduction of network partitions, disk corruption
- **VFS Abstraction**: Production and test code paths identical

**Core Engine**: LSM-tree with WAL, decomposed into `MemtableManager` (in-memory) and `SSTableManager` (on-disk) coordinators.

## Quick Start

```bash
# Install exact Zig version + git hooks
./scripts/install_zig.sh
./scripts/setup_hooks.sh

# Build and test everything
./zig/zig build test

# Run CortexDB server
./zig/zig build run
```

## Key Commands

```bash
./zig/zig build test        # Complete test suite (unit + simulation + integration)
./zig/zig build run         # Start CortexDB server
./zig/zig build simulation  # Deterministic failure scenario tests
./zig/zig build benchmark   # Performance validation
./zig/zig build fuzz        # Fuzz testing
./zig/zig build check       # Quick compilation + quality checks
```

## Features

- **Sub-millisecond Latency**: Optimized for LLM context injection
- **Deterministic Testing**: Byte-for-byte reproducible disaster scenarios
- **Memory Safety**: Arena-per-subsystem eliminates use-after-free by design
- **Zero Dependencies**: Pure Zig, self-contained
- **Defensive Programming**: Comprehensive assertion framework catches bugs early

## Documentation

- **[DESIGN.md](docs/DESIGN.md)**: Architecture and philosophy
- **[DEVELOPMENT.md](docs/DEVELOPMENT.md)**: Build system and debugging
- **[STYLE.md](docs/STYLE.md)**: Code standards and patterns

## License

[MIT](LICENSE)
