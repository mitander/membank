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

### What it feels like to use CortexDB

The goal is dead simple: give your LLM the **right context**, not just **more context**.

```zig
// Find that function you're debugging
var main_func = try cortex.find("function:main");

// Get everything it touches, 3 levels deep
var context = try cortex.traverse(main_func.id, .outgoing, .depth(3));

// Feed it to your LLM with confidence
var prompt = try llm.prompt("Explain the call graph for this function:", context);
```

**Why this matters**: Instead of dumping 50 random code chunks into your prompt, you get the 5 functions that `main()` actually calls, the 3 files they import, and the config they read. **Structured knowledge** beats **scattered information** every time.

### Real-world Example: Code Review Assistant

```zig
// Someone changed the authentication logic
var auth_func = try cortex.find("function:authenticate_user");

// What else might be affected?
var dependencies = try cortex.traverse(auth_func.id, .incoming, .depth(2));

// What does it call?
var implementations = try cortex.traverse(auth_func.id, .outgoing, .depth(1));

// Now you can ask your LLM intelligent questions:
var review = try llm.prompt(
    "This function changed. What security implications should I consider?",
    .{ .changed = auth_func, .callers = dependencies, .calls = implementations }
);
```

**The difference**: Your LLM isn't guessing. It **knows** that changing `authenticate_user()` affects the login endpoint, the session middleware, and the API gateway. That's the power of a knowledge graph.

### Quick Start

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
