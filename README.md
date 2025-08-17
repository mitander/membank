# KausalDB

[![CI](https://github.com/kausaldb/kausaldb/actions/workflows/ci.yml/badge.svg)](https://github.com/kausaldb/kausaldb/actions)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> _Code is a graph. Query it._

A database built for the way code actually works—not as flat text, but as a living graph of dependencies, calls, and causal relationships. KausalDB lets Large Language Models reason about software with the ground truth of its architecture.

## Quick Start

```bash
# Get the source
git clone https://github.com/kausaldb/kausaldb
cd kausaldb

# Install toolchain and hooks
./scripts/install_zig.sh
./scripts/setup_hooks.sh

# Build and test
./zig/zig build test

# Start the database
./zig/zig build run
```

Connect and query:

```zig
const kausal = @import("kausaldb");

// Find a function and trace its impact
var func = try db.find("authenticate_user");
var affected = try db.traverse(func.id, .incoming, .depth(3));
var deps = try db.traverse(func.id, .outgoing, .depth(2));

// Now your LLM has the actual dependency subgraph
var context = ContextSubgraph{ func, affected, deps };
```

## The Problem

Standard RAG is broken for complex systems. Feeding an LLM chunks of "similar" text is guesswork. Code has structure—imports, calls, dependencies—that semantic search can't capture. You need the causal graph.

## The Solution

Instead of approximating with embeddings, KausalDB models the actual relationships in your codebase. When a function changes, you can precisely query what's affected upstream and downstream. No guessing. No hallucination. Just the graph.

## Design Philosophy

We build for purpose, not popularity. Every decision optimizes for modeling code as a queryable graph:

- **Zero-cost abstractions**: Safety with no runtime overhead
- **Single-threaded core**: Eliminates races by design
- **Arena memory model**: O(1) cleanup, zero leaks
- **Simulation-first testing**: Validates against disk corruption, network partitions, power loss
- **LSM-tree storage**: Optimized for high-volume code ingestion

_Performance: 76K writes/sec, 34M reads/sec, sub-microsecond block access_

## Development

Run the full validation suite (mirrors CI):

```bash
./scripts/local_ci.sh
```

Chaos testing with different intensity levels:

```bash
./scripts/fuzz.sh quick     # 500K iterations, ~30s
./scripts/fuzz.sh deep      # 10M iterations, ~10min
./scripts/fuzz.sh production # Continuous with monitoring
```

Performance analysis:

```bash
./zig/zig build benchmark && ./zig-out/bin/benchmark all
```

## Architecture

**Core Documents:**

- [**Design Philosophy**](docs/DESIGN.md) - The why and how of KausalDB
- [**Memory Architecture**](docs/architecture/memory-model.md) - Five-level memory hierarchy
- [**Query Engine**](docs/architecture/query-engine.md) - Graph traversal algorithms
- [**Development Guide**](docs/DEVELOPMENT.md) - Build system, workflows, debugging

**Implementation Guides:**

- [**Coding Style**](docs/STYLE.md) - Naming, patterns, memory management
- [**Testing Philosophy**](docs/TESTING_GUIDELINES.md) - Simulation-first approach
- [**Performance Analysis**](docs/PERFORMANCE.md) - Benchmarking and regression detection

---

Built with [Zig](https://ziglang.org). No dependencies. Single binary. Zero runtime.
