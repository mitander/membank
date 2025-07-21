# CortexDB

[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![CI Status](https://github.com/mitander/cortexdb/actions/workflows/ci.yml/badge.svg)](https://github.com/mitander/cortexdb/actions)

A structured and deterministic context database for Large Language Models. Built in Zig for
mission-critical performance and reliability.

CortexDB provides LLMs with an extensive and auto-updated knowledge base, ensuring they operate
on fresh, relevant, and well-structured context.

## Architecture

CortexDB is designed as a modular Zig project with strict boundaries between components to
enable deterministic simulation and testing.

```
cortex-core (Database engine, storage, replication)
    ├── cortex-vfs (Virtual File System for deterministic I/O)
    ├── cortex-sim (Deterministic simulation framework)
    ├── cortex-client (Client library for applications)
    └── cortex-cli (Command-line interface)
```

**Core Features:**

- **Structured Context:** Organizes knowledge into versioned "Context Blocks" with typed
  relationships, forming a queryable knowledge graph.
- **Auto-Update Engine:** Ingestion pipelines monitor data sources (e.g., Git repositories,
  documentation sites) and automatically update the context.
- **Deterministic by Design:** Built on a Virtual File System (VFS) with comprehensive
  simulation framework, allowing for byte-for-byte reproducible testing of complex
  distributed scenarios.
- **Defensive Programming:** Comprehensive assertion framework catches bugs early with
  strategic runtime checks in debug builds.
- **Optimized for LLM Retrieval:** Low-latency query engine designed to feed context into
  LLM prompts efficiently.
- **Built in Zig:** Guarantees performance, safety, and simplicity through explicit memory
  management and `comptime` metaphysics.

## The CortexDB Model

CortexDB doesn't just store text; it stores structured knowledge.

- **Context Blocks:** The atomic unit of knowledge. A block can be a function, a class, a
  document paragraph, or any logical chunk of information. Each block has a unique ID,
  content, and metadata.
- **Knowledge Graph:** Blocks are connected via labeled edges (e.g., `IMPORTS`, `DEFINED_IN`,
  `REFERENCES`), allowing the query engine to traverse relationships and retrieve rich,
  interconnected context.

This structured approach solves the "context drift" problem where an LLM loses focus in a sea
of unstructured text.

## Usage

```bash
# Build the project (includes running all tests)
zig build

# Run the demo showing storage and query functionality
zig build && ./zig-out/bin/cortexdb demo

# Start the CortexDB server
zig build && ./zig-out/bin/cortexdb server

# Show version information
./zig-out/bin/cortexdb version

# Show help
./zig-out/bin/cortexdb help

# Run comprehensive test suite
zig build test
```

## Dependencies

**Required:** None. CortexDB is a self-contained, dependency-free Zig project.

## Development

### Setup

CortexDB requires a specific Zig version. Install the project Zig to ensure consistency:

```bash
# Install project-specific Zig version (if needed)
./scripts/install-zig.sh

# Use project Zig directly for consistency
./zig/zig build
./zig/zig build test

# Or use system Zig if you have the correct version
zig build
```

### Workflow

Quality is enforced through a series of automated checks. Every commit is expected to pass
these gates.

```bash
# Standard development workflow
zig build                # Build the project
zig fmt .               # Format all source files
zig build test          # Run all tests
zig build tidy          # Run code quality checks

# Run the demo to test functionality
zig build && ./zig-out/bin/cortexdb demo
```

**Style:** Enforced by `./zig/zig fmt`. All other conventions are documented in `docs/STYLE.md`. We
favor simple interfaces, explicit error handling, and a deep aversion to hidden allocations
or control flow.

**Testing:** Unit tests, comprehensive assertion framework, and most importantly, deterministic
simulation scenarios that test the system as a whole under failure conditions including
network partitions, packet loss, and byzantine faults.

## Implementation Details

- **Engine:** Single-threaded, asynchronous design leveraging Zig's `async/await`. Modeled
  after high-performance financial databases.
- **Storage:** A custom log-structured merge-tree (LSMT) tailored for our specific data model.
  All I/O is batched and routed through the VFS.
- **VFS (Virtual File System):** A vtable-based interface that abstracts the file system. The
  production implementation maps to the OS, while the simulation implementation uses an
  in-memory data structure, enabling deterministic tests with controllable time progression
  and network conditions.
- **API:** A simple, high-performance binary protocol for client-server communication.

## Documentation

- [Design](docs/DESIGN.md) - Architecture and implementation rationale.
- [Style](docs/STYLE.md) - Coding standards and conventions.
- [Architecture](docs/architecture/overview.md) - Detailed architecture specifications.

## License

MIT License
