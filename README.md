# kausaldb

[![CI Status](https://github.com/kausaldb/kausaldb/actions/workflows/ci.yml/badge.svg)](https://github.com/kausaldb/kausaldb/actions)
[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> Code is a graph. Query it.

KausalDB is a database built to give Large Language Models (LLMs) a structural understanding of code. It models a codebase not as a flat directory of text files, but as a directed graph of its dependencies, structures, and relationships, allowing an AI to reason about software with the ground truth of its architecture.

## Quick Start

```bash
# Clone repository
git clone https://github.com/kausaldb/kausaldb
cd kausaldb

# Install toolchain
./scripts/install_zig.sh
./scripts/setup_hooks.sh

# Run tests
./zig/zig build test

# Start server
./zig/zig build run
```

## Why KausalDB?

Standard Retrieval-Augmented Generation (RAG) is flawed for complex systems. Feeding an LLM a flat list of semantically similar text chunks is an approximation. KausalDB provides the ground truth. Instead of guessing, your agent can execute precise, structural queries that reveal the causal relationships within your code.

```zig
// A function was modified. What systems are causally affected?
var func = try kausaldb.find("function:authenticate_user");

// Find callers and downstream dependencies.
var callers = try kausaldb.traverse(func.id, .incoming, .depth(3));
var dependencies = try kausaldb.traverse(func.id, .outgoing, .depth(2));

// Feed the interconnected subgraph to the reasoning layer.
var analysis = try llm.prompt(
    "Analyze the security implications of this change given its context.",
    .{ .change = func, .callers = callers, .dependencies = dependencies }
);
```

The model now operates on a subgraph of reality, eliminating guesswork and enabling a deeper level of analysis.

## Features

- **Fast:** 47K writes/sec and 16.7M reads/sec.
- **Zero Races by Design:** A single-threaded core with an async I/O event loop eliminates data races and simplifies state management.
- **Zero Dependencies:** A single, static binary written in pure Zig. No runtime, no complex toolchains, no headache.
- **Deterministic Testing:** A simulation-first testing framework validates the exact production code against catastrophic failures like disk corruption, network partitions, and power loss in a byte-for-byte reproducible manner.
- **Arena-Based Memory Model:** An arena-per-subsystem memory model provides O(1) cleanup for state-heavy components, high performance, and zero memory leaks across a test suite of over 500 tests.

## Development

The repository includes a full suite of tools for validation and performance analysis. Our `pre-push` Git hook runs these checks automatically to catch most CI failures locally.

```bash
# Run the full validation suite (mimics CI)
./scripts/local_ci.sh

# Run performance benchmarks manually
./zig/zig build benchmark
./zig-out/bin/benchmark storage

# Run chaos testing with random inputs (multiple profiles available)
./scripts/fuzz.sh quick
```

## Performance Monitoring

KausalDB includes comprehensive performance monitoring with automated regression detection:

```bash
# Run performance benchmarks manually  
./zig-out/bin/benchmark all

# View performance dashboard
cat .github/performance-dashboard.md

# Run performance tests manually  
./zig/zig build test-performance
```

**Current Status**: [Performance Dashboard](.github/performance-dashboard.md) | [Latest Results](.github/performance-baseline.json)

## Documentation

- **[Architectural Design](docs/DESIGN.md)**
- **[Development Workflow](docs/DEVELOPMENT.md)**
- **[Style Guide](docs/STYLE.md)**
- **[Performance Dashboard](.github/performance-dashboard.md)**
