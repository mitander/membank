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

# Install Zig toolchain and setup Git hooks
./scripts/setup.sh

# Build and run the server
./zig/zig build run
```

The server starts on port 8080. You can now ingest code and run graph queries:

```bash
# Example: Ingest a Zig project
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"source": {"type": "git", "path": "/path/to/zig/project"}}'

# Query function dependencies
curl http://localhost:8080/query/traverse/function:main/outgoing/3
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
# Run fast unit tests
./zig/zig build test

# Run comprehensive test suite (includes simulation, fault injection)
./zig/zig build test-all

# Run benchmarks
./zig/zig build benchmark

# Format code
./zig/zig build fmt

# Quality checks
./zig/zig build tidy
```

Tests run the exact production code against simulated disk corruption, network partitions, and power loss. No mocks.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines and [HACKING.md](HACKING.md) for architecture deep-dive.
