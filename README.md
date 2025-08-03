# kausaldb

[![CI Status](https://github.com/kausaldb/kausaldb/actions/workflows/ci.yml/badge.svg)](https://github.com/kausaldb/kausaldb/actions)
[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> Code is a graph. Query it.

Kausal is a purpose-built knowledge graph for **Large Language Models (LLMs)**. It allows your AI to reason about your codebase as a graph of cause and effect.

## Bootstrap

```bash
# Install required Zig version & git hooks
./scripts/install_zig.sh
./scripts/setup_hooks.sh

# Build & validate
./zig/zig build test

# Execute server
./zig/zig build run
```

## Why?

Standard Retrieval-Augmented Generation (RAG) is flawed. Feeding an LLM a list of semantically similar text chunks is an approximation that fails in complex systems like codebases. Kausal provides ground truth.

Instead of guessing, your LLM can execute precise, structural queries.

```zig
// An auth function was modified.
var auth_func = try kausal.find("function:authenticate_user");

// What systems are causally affected by this change?
var callers = try kausal.traverse(auth_func.id, .incoming, .depth(2));
var dependencies = try kausal.traverse(auth_func.id, .outgoing, .depth(1));

// Feed the interconnected facts to the reasoning layer.
var analysis = try llm.prompt(
    "Analyze security implications of this change given its context.",
    .{ .change = auth_func, .callers = callers, .dependencies = dependencies }
);
```

The LLM now operates on a subgraph of reality, not a flat list of disconnected files. This eliminates guesswork.

## Architectural Principles

Kausal is not a general-purpose database. Every architectural decision is optimized for providing a reliable, high-performance context engine.

* **An Opinionated Data Model:** `ContextBlock` and `GraphEdge` are not generic nodes and vertices. They are specific primitives for representing structured knowledge and its explicit connections.

* **A Write-Optimized Core:** The LSM-Tree architecture is designed for the high-volume ingestion required to keep the graph synchronized with evolving systems.

* **Deterministic by Design:** A single-threaded core guarantees zero data races. A simulation-first testing model validates the system against catastrophic failures, providing the reliability necessary to be a source of ground truth.

* **Zero Dependencies:** Compiles to a single static binary. No runtime, no complex deployments. Pure Zig.

## Development

A full suite of development and validation tools is included.

```bash
# Standard test cycle (fast)
./zig/zig build test

# Full validation suite (CI-level)
./zig/zig build test-all

# Performance regression detection
./zig/zig build benchmark

# Chaos testing with random inputs
./zig/zig build fuzz
```

## Documentation

- **[Architectural Design](docs/DESIGN.md)**
- **[Development & Testing](docs.DEVELOPMENT.md)**
- **[Obsessive Style Disorder](docs/STYLE.md)**
