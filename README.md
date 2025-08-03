# kausaldb

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI Status](https://github.com/kausaldb/kausaldb/actions/workflows/ci.yml/badge.svg)](https://github.com/kausaldb/kausaldb/actions)

> A queryable graph of cause and effect.

Kausal models a codebase as a directed graph, allowing for precise, structural queries against the system's causal layer. The result is surgical context, not a data dump.

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

Code is a network of dependencies. Current tools treat it as flat text. This is a fundamental mismatch. They find text, not causality.

When `authenticate_user()` is modified, the critical question isn't "what other text files contain this string?" but "what systems are causally affected by this change?"

Kausal answers the second question.

```zig
// An auth function was modified.
var auth_func = try kausal.find("function:authenticate_user");

// Identify its upstream callers.
var callers = try kausal.traverse(auth_func.id, .incoming, .depth(2));

// Identify its downstream dependencies.
var dependencies = try kausal.traverse(auth_func.id, .outgoing, .depth(1));

// Feed the interconnected facts to the reasoning layer.
var analysis = try llm.prompt(
    "Analyze security implications of this change given its context.",
    .{ .change = auth_func, .callers = callers, .dependencies = dependencies }
);
```

The LLM now operates on a subgraph of reality - the login endpoint, session middleware, API gateway - instead of a list of text files. This eliminates guesswork.

## Design Principles

Engineered for determinism and reliability.

- **Sub-millisecond Reads:** Achieved via a write-optimized LSM-tree storage layer.
- **Deterministic Simulation Testing:** The core is validated against simulated network failures and data corruption across 500+ test cases.
- **Single-Threaded Core:** Guarantees zero data races by design, eliminating an entire class of concurrency bugs.
- **Arena Allocation:** Enforces strict memory management, preventing common memory-safety vulnerabilities.
- **Zero Dependencies:** Compiles to a single static binary. No runtime, no complex deployments. Pure Zig.

## Development

```bash
# Standard test cycle
./zig/zig build test

# Run server binary
./zig/zig build run

# Full validation suite
./zig/zig build test-all    # Includes stress and simulation tests
./zig/zig build benchmark   # Performance regression checks
./zig/zig build fuzz        # Chaos testing via random inputs
```

## Documentation

- **[Architectural Design](docs/DESIGN.md)**
- **[Development & Testing](docs.DEVELOPMENT.md)**
- **[Obsessive Style Disorder](docs/STYLE.md)**
