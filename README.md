# kausaldb

[![CI Status](https://github.com/kausaldb/kausaldb/actions/workflows/ci.yml/badge.svg)](https://github.com/kausaldb/kausaldb/actions)
[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> Code is a graph. Query it.

KausalDB models your codebase as a directed graph of dependencies and relationships. Built for LLMs that need to understand software structure, not just grep through text.

## System Requirements

- Linux, macOS (Windows support planned)
- 4GB+ RAM recommended
- No external dependencies (single static binary)

## Quick Start

```bash
git clone https://github.com/kausaldb/kausaldb
cd kausaldb

# One-time setup (installs Zig toolchain and Git hooks)
./scripts/setup.sh

# Build and start the server
./zig/zig build server

# Or run a quick demo
./zig/zig build demo
```

The server starts on port 8080 with a binary protocol for client connections.

## Why

Your codebase isn't a flat directory of text files. It's a network of dependencies with causal relationships. Grep and semantic search find text. KausalDB finds causality.

```zig
// What breaks if we change authenticate_user()?
var func = try kausaldb.find("function:authenticate_user");
var affected = try kausaldb.traverse(func.id, .incoming, .depth(3));
````

The model operates on ground truth, not approximations.

## Performance

Single-threaded by design. Zero data races, deterministic testing.

- **Block Write**: 30µs (33K ops/sec optimized)
- **Block Read**: 33ns (29.9M ops/sec from memtable)
- **Single Query**: 56ns (17.9M ops/sec)
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
# Quick development cycle
./zig/zig build                   # Build all executables
./zig/zig build server            # Start server (Ctrl+C to stop)
./zig/zig build demo              # Run demo
./zig/zig build analyze           # Self-analysis demo
./zig/zig build bench-write       # Run specific benchmark

# Testing and validation
./zig/zig build test              # Fast tests (unit + integration)
./zig/zig build test-all          # Full test suite (simulation, fault injection)
./zig/zig build perf              # Validate performance thresholds

# Code quality
./zig/zig build fmt-fix           # Fix formatting
./zig/zig build tidy              # Check code quality
./zig/zig build ci                # Run all CI checks
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines and [HACKING.md](HACKING.md) for architecture deep-dive.
