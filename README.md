# Membank

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI Status](https://github.com/mitander/membank/actions/workflows/ci.yml/badge.svg)](https://github.com/mitander/membank/actions)

**The knowledge graph database built for LLMs.**

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

**Your LLM is flying blind.** It gets 50 random functions in a prompt and hopes the right ones are there. No understanding of what calls what, no knowledge of dependencies, no grasp of the bigger picture.

**Result:** Suggestions that break your authentication system because the LLM didn't know what depends on it.

Membank fixes this with **knowledge graphs**:

- **Context Blocks**: Logical units (functions, classes, docs) with metadata
- **Typed Relationships**: `CALLS`, `IMPORTS`, `REFERENCES` — your LLM knows the connections
- **Graph Traversal**: Get exactly what matters, not random text

**Goal**: Give your LLM the **right context**, not just **more context**.

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

**The difference:** Your LLM isn't guessing anymore. It **knows** exactly what `authenticate_user()` touches — the login endpoint, session middleware, API gateway. **No more broken suggestions. No more missed dependencies.**

## Why Membank?

- **Sub-millisecond queries** — Real-time context injection that doesn't slow down your LLM
- **Actually reliable** — 500+ deterministic tests simulate network failures, disk corruption, memory pressure
- **Production-ready** — Arena memory model eliminates entire bug classes, zero hidden allocations
- **Zero dependencies** — Pure Zig, single binary, no complex deployment
- **Proven patterns** — Built like financial trading systems (because they have to work)

## Architecture

**Built like financial trading systems — fast, reliable, deterministic.**

- **LSM-tree storage** — Optimized for write-heavy ingestion with blazing fast reads
- **Single-threaded core** — Data races are impossible by design
- **Arena memory model** — Bulk allocation/deallocation eliminates memory bugs
- **Virtual file system** — Same production code runs in deterministic tests
- **Simulation testing** — Reproduce catastrophic failures deterministically

> *"Your LLM finally has a memory that doesn't suck. We're as excited about this as you should be."*

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

- **[DESIGN.md](docs/DESIGN.md)**: Architecture and philosophy
- **[DEVELOPMENT.md](docs/DEVELOPMENT.md)**: Build system and debugging
- **[STYLE.md](docs/STYLE.md)**: Code standards and patterns

## License

[MIT](LICENSE)
