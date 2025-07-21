# CortexDB

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI Status](https://github.com/mitander/cortexdb/actions/workflows/ci.yml/badge.svg)](https://github.com/mitander/cortexdb/actions)

**A fast, deterministic context database for Large Language Models.**

CortexDB is born from a love for building foundational, high-performance software. It's a new take on a critical piece of the AI puzzle: giving LLMs a brain with a long-term, reliable memory. This is a specialized database, built in Zig with an obsessive focus on performance, simplicity, and rock-solid reliability.

---

## What is CortexDB?

Modern LLM applications suffer from "context drift." They operate on a tiny, transient window of information, quickly losing track of the bigger picture. CortexDB solves this. It acts as an external knowledge base, feeding the LLM with fresh, relevant, and deeply interconnected context in milliseconds.

Instead of just storing text, CortexDB stores structured knowledge:

- **Context Blocks:** The atomic unit of knowledge. A block isn't just a string; it's a logical chunk of information—a function, a document paragraph, a configuration setting—with a unique ID, rich metadata, and a version history.
- **The Knowledge Graph:** Blocks are linked by typed edges (`IMPORTS`, `DEFINED_IN`, `REFERENCES`). This allows the query engine to traverse relationships, retrieving not just one piece of information, but a rich, interconnected graph of relevant context for the LLM to reason about.

## The Philosophy: Why Zig? Why This Way?

The choice of Zig is deliberate. It reflects a core belief: **simplicity is the prerequisite for reliability.**

CortexDB is heavily inspired by the design of high-performance financial databases like TigerBeetle. This project is built on a few strong opinions about how to engineer mission-critical software:

1.  **Explicit is Better Than Implicit:** Zig has no hidden control flow and no hidden memory allocations. This forces a level of clarity and discipline that eliminates entire categories of bugs. That's a feature.
2.  **Performance is Designed, Not Tweaked:** Speed is achieved through architecture, not late-stage optimization hacks. The core engine is single-threaded, uses an event loop for async I/O, and relies on an arena-based memory model to all but eliminate dynamic allocations on the hot path.
3.  **The Testing Philosophy: Don't Mock, Simulate.** The only way to trust a complex system is to test it holistically. The entire database is built on a Virtual File System (VFS), enabling byte-for-byte reproducible tests of network partitions, disk corruption, and other disasters.

## Quickstart: 3 Steps to Get Going

Get from `git clone` to a running demo in under a minute.

**1. Clone the repo:**

```bash
git clone https://github.com/cortexdb/cortexdb.git
cd cortexdb
```

**2. Install the toolchain:**
The included scripts ensure you have the exact Zig version needed for the project.

```bash
./scripts/install-zig.sh
./scripts/setup-hooks.sh
```

**3. Build and Test:**
This command builds the project and runs the comprehensive test suite.

```bash
./zig/zig build test
```

**4. Run CortexDB:**

```bash
./zig/zig build run
```

## Core Features

- **Absurdly Fast:** Designed for low-latency queries to feed context into LLM prompts with minimal overhead.
- **Deterministic by Design:** A simulation-first testing framework allows for byte-for-byte reproducible testing of complex distributed scenarios.
- **Robust Memory Safety:** A simple, `ArenaAllocator`-based memory model eliminates entire classes of memory corruption bugs by design.
- **Log-Structured Merge-Tree:** The storage engine is a custom LSMT optimized for high-volume ingestion and fast writes.
- **Self-Contained:** Zero external dependencies. The entire project is pure Zig.
- **Defensive Programming:** A comprehensive assertion framework (`src/assert.zig`) catches invariants and logic bugs early in development.

## Project Status

CortexDB is in the **early, active development** phase. The core storage engine, WAL, in-memory index (memtable), and query-by-ID functionality are complete and stable, with a 100% pass rate across the comprehensive test suite.

Current focus is on building out the **Ingestion Pipeline** and a pragmatic **Replication Layer**. See the [Design Document](docs/DESIGN.md) for the full architectural vision.

## Development

Contributions are welcome. The goal is a frictionless development experience. The single most important command is `./zig/zig build test`, which builds and runs the entire test suite.

**Available Commands:**

- `./zig/zig build test` - Run all tests
- `./zig/zig build run` - Run CortexDB server
- `./zig/zig build check` - Quick compilation check and quality checks
- `./zig/zig build benchmark` - Performance benchmarks
- `./zig/zig build fuzz` - Fuzz testing
- `./zig/zig build tidy` - Code quality checks
- `./zig/zig build fmt` - Check formatting
- `./zig/zig build ci` - Complete CI pipeline

**Test Categories:**

- `./zig/zig build unit-test` - Unit tests
- `./zig/zig build simulation` - Simulation tests
- `./zig/zig build storage_simulation` - Storage simulation tests
- `./zig/zig build wal_recovery` - WAL recovery tests
- `./zig/zig build wal_memory_safety` - WAL memory safety tests
- `./zig/zig build memory_isolation` - Memory isolation tests
- `./zig/zig build integration` - Integration tests

For a deeper dive into the workflow, debugging tools, and commit standards, please read the **[Development Guide](docs/DEVELOPMENT.md)**.

## Documentation

- **[DESIGN.md](docs/DESIGN.md):** The "why." The architecture and philosophy of the project.
- **[DEVELOPMENT.md](docs/DEVELOPMENT.md):** The "how." The development workflow, toolchain, and debugging guide.
- **[STYLE.md](docs/STYLE.md):** The coding standards and conventions.

## License

This project is licensed under the **[MIT License](LICENSE)**.
