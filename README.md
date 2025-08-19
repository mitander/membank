# kausaldb

[![CI Status](https://github.com/kausaldb/kausaldb/actions/workflows/ci.yml/badge.svg)](https://github.com/kausaldb/kausaldb/actions)
[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> Code is a graph. Query it.

KausalDB models your codebase as a directed graph of dependencies and relationships. Built for LLMs that need to understand software structure, not just grep through text.

## Quick Start
```bash
git clone https://github.com/kausaldb/kausaldb
cd kausaldb

./scripts/install_zig.sh
./zig/zig build run
```

## Why

Your codebase isn't a flat directory of text files. It's a network of dependencies with causal relationships. Grep and semantic search find text. KausalDB finds causality.

```zig
// What breaks if we change authenticate_user()?
var func = try kausaldb.find("function:authenticate_user");
var affected = try kausaldb.traverse(func.id, .incoming, .depth(3));
```

The model operates on ground truth, not approximations.

## Performance

Single-threaded by design. Zero data races, deterministic testing.

- **Block Write**: 15µs
- **Block Read**: 0.08µs
- **Graph Traversal**: <100µs for 3-hop queries
- **Zero Dependencies**: Single static binary

## Architecture

LSM-Tree storage with arena-based memory management. See [DESIGN.md](docs/DESIGN.md) for the philosophy.

```
src/
├── core/       # Types, assertions, memory patterns
├── storage/    # LSM-Tree implementation
├── query/      # Graph traversal engine
└── server/     # Binary protocol server
```

## Development

```bash
# Run tests (includes deterministic failure simulation)
./zig/zig build test

# Run benchmarks
./zig/zig build benchmark

# Check for regressions
./scripts/check_regression.sh
```

Tests run the exact production code against simulated disk corruption, network partitions, and power loss. No mocks.
