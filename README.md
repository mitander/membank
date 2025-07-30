# membank

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI Status](https://github.com/mitander/membank/actions/workflows/ci.yml/badge.svg)](https://github.com/mitander/membank/actions)

**The knowledge graph database built for LLMs.**
> *"Your LLM finally has a memory that doesn't suck. We're as excited about this as you should be."*

Stop feeding your LLM random text chunks. Membank gives you **structured, interconnected knowledge** with sub-millisecond queries. Your AI finally knows what's connected to what.

### Quick Start

```bash
# Install exact Zig version + git hooks
./scripts/install_zig.sh
./scripts/setup_hooks.sh

# Build and test everything
./zig/zig build test

# Run Membank server
./zig/zig build run
```

## The Problem

**Your LLM doesn't know what's connected to what.** You dump 50 functions into a prompt hoping the relevant ones are included. When your LLM suggests changing `authenticate_user()`, it has no idea what breaks.

**Membank gives your LLM a knowledge graph** instead of random text chunks:

- **Context Blocks** with metadata and relationships
- **Graph Traversal** to find exactly what matters
- **Complete picture** of dependencies and callers

**Goal**: Right context, not more context.

```zig
// Someone changed authentication logic
var auth_func = try membank.find("function:authenticate_user");

// What calls this function?
var callers = try membank.traverse(auth_func.id, .incoming, .depth(2));

// What does this function call?
var dependencies = try membank.traverse(auth_func.id, .outgoing, .depth(1));

// Now your LLM has the complete picture:
var review = try llm.prompt(
    "This function changed. What security implications should I consider?",
    .{ .changed = auth_func, .callers = callers, .calls = dependencies }
);
```

**The difference:** Your LLM isn't guessing anymore. It **knows** exactly what `authenticate_user()` touches - the login endpoint, session middleware, API gateway.

**No more broken suggestions. No more missed dependencies.**

## Built to be Fast and Reliable

**Deterministic by design. Built like financial trading systems.**

- **Sub-millisecond queries** via LSM-tree storage optimized for write-heavy ingestion
- **Actually reliable** with 500+ deterministic tests simulating network failures and corruption
- **Zero data races** with single-threaded core architecture
- **Memory safe** using arena allocation model that eliminates entire bug classes
- **Zero dependencies** — Pure Zig, single binary, no complex deployment
- **Simulation tested** — Same production code runs in deterministic failure scenarios

## Development

```bash
# Fast development cycle
./zig/zig build test        # Unit tests (~30s)
./zig/zig build run         # Start server

# Full validation
./zig/zig build test-all    # All tests including stress/simulation
./zig/zig build benchmark   # Performance regression detection
./zig/zig build fuzz        # Chaos testing with random inputs
```


## Documentation

- **[DESIGN](docs/DESIGN.md)**: Architecture and philosophy
- **[DEVELOPMENT](docs/DEVELOPMENT.md)**: Build system and debugging
- **[STYLE](docs/STYLE.md)**: Code standards and patterns

