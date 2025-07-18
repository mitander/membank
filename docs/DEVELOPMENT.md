# CortexDB Development Guide

Development setup and workflow for CortexDB contributors.

## Prerequisites

### Zig Installation

CortexDB requires Zig 0.15.0 or later.

**Option 1: Official installer**
```bash
# Download from https://ziglang.org/download/
# Extract and add to PATH
```

**Option 2: Using the project script**
```bash
./zig/download.sh
export PATH="$PWD/zig:$PATH"
```

**Option 3: Package manager**
```bash
# macOS
brew install zig

# Ubuntu/Debian (may be outdated)
sudo apt install zig

# Arch Linux
sudo pacman -S zig
```

Verify installation:
```bash
zig version
# Should output: 0.15.0 or later
```

### Git Configuration

Configure git for conventional commits:
```bash
git config --local commit.template .gitmessage
```

## Getting Started

### Clone and Build

```bash
git clone <repository-url>
cd cortexdb

# Build the project
zig build

# Run tests
zig build test

# Run all quality checks
zig build tidy
```

### Project Structure

```
cortexdb/
├── src/                    # Source code
│   ├── main.zig           # Main entry point
│   ├── query_engine.zig   # Query processing
│   ├── block_storage.zig  # Block storage
│   ├── benchmark.zig      # Performance testing
│   ├── fuzz.zig          # Fuzz testing
│   ├── tidy.zig          # Code quality checker
│   └── shell.zig         # Shell utilities
├── tests/                 # Test files
├── docs/                  # Documentation
│   ├── DESIGN.md         # Architecture design
│   ├── STYLE.md          # Code style guide
│   └── DEVELOPMENT.md    # This file
├── build.zig             # Build configuration
├── build.zig.zon         # Dependencies
└── .github/workflows/    # CI configuration
```

## Development Workflow

### Daily Workflow

1. **Pull latest changes**
   ```bash
   git pull origin main
   ```

2. **Create feature branch**
   ```bash
   git checkout -b feat/your-feature-name
   ```

3. **Write code following style guide**
   - Read `docs/STYLE.md` for conventions
   - Use `zig fmt` for formatting
   - Follow TigerBeetle-inspired naming patterns

4. **Test your changes**
   ```bash
   # Format code
   zig fmt .

   # Build and test
   zig build
   zig build test

   # Run quality checks
   zig build tidy

   # Run specific test
   zig test src/your_file.zig
   ```

5. **Commit with conventional format**
   ```bash
   git add .
   git commit -m "feat(component): add new functionality"
   ```

### Build Targets

```bash
# Basic build
zig build

# Run all tests
zig build test

# Code quality checks
zig build tidy

# Performance benchmarks
zig build benchmark
zig run zig-out/bin/benchmark block_validation

# Fuzz testing
zig build fuzz
zig run zig-out/bin/fuzz storage 10000

# Individual tools
zig run src/main.zig -- version
zig run src/tidy.zig
```

### Testing

#### Unit Tests
```bash
# Run all tests
zig build test

# Run specific file tests
zig test src/query_engine.zig

# Verbose test output
zig build test -- --summary all
```

#### Simulation Tests
```bash
# Run deterministic simulations
zig build test  # Includes simulation tests

# Run only simulation tests
zig test tests/simulation_test.zig

# Test individual components
zig test src/vfs.zig
zig test src/simulation.zig
zig test src/assert.zig
```

#### Benchmarks
```bash
# All benchmarks
zig run zig-out/bin/benchmark all

# Specific benchmarks
zig run zig-out/bin/benchmark block_validation
zig run zig-out/bin/benchmark query_processing
```

#### Fuzz Testing
```bash
# All targets
zig run zig-out/bin/fuzz all 50000

# Specific targets
zig run zig-out/bin/fuzz block_parser 10000 42
zig run zig-out/bin/fuzz query_engine 10000 123
zig run zig-out/bin/fuzz storage 10000 456
```

### Code Quality

#### Formatting
```bash
# Check formatting
zig fmt --check .

# Auto-format
zig fmt .
```

#### Style Checks
```bash
# Run tidy checker
zig build tidy
zig run src/tidy.zig

# Manual pattern checks
grep -r "std\.BoundedArray" src/  # Should be empty
grep -r "FIXME\|TODO" src/       # Should be resolved
```

#### Documentation
- All public functions must have doc comments
- Use `///` for function documentation
- Use `//!` for module documentation
- Focus on "why" not "what" in comments

### Performance Guidelines

#### Memory Management
- Pass allocators explicitly
- Use arena allocators for temporary data
- Avoid allocations in hot paths
- Document allocation patterns

#### Assertions

CortexDB uses a comprehensive assertion framework (`src/assert.zig`) for defensive programming:

```bash
# Test assertion framework
zig test src/assert.zig
```

Key assertion patterns:
- `assert()` - General condition with descriptive message (debug only)
- `assert_always()` - Critical safety assertions (always active)
- `assert_buffer_bounds()` - Prevent buffer overflows
- `assert_range()` - Validate value ranges
- `assert_state_valid()` - Verify state transitions
- `assert_index_valid()` - Check array bounds

Example usage:
```zig
const assert = @import("assert.zig");

pub fn process_block(block: []const u8, index: usize) !void {
    assert.assert_not_empty(block, "Block cannot be empty", .{});
    assert.assert_index_valid(index, MAX_BLOCKS, "Index {} >= {}", .{ index, MAX_BLOCKS });
    assert.assert_buffer_bounds(0, block.len, BUFFER_SIZE, "Buffer overflow: {} > {}", .{ block.len, BUFFER_SIZE });
    
    // Process block...
}
```

#### Deterministic Behavior

CortexDB uses a deterministic simulation framework for testing:

**Key Components:**
- **VFS Interface** (`src/vfs.zig`): Abstracts file system operations
- **Simulation Harness** (`src/simulation.zig`): Manages nodes, network, and time
- **Simulation VFS** (`src/simulation_vfs.zig`): In-memory file system for testing

**Writing Simulation Tests:**
```zig
test "network partition scenario" {
    const allocator = std.testing.allocator;
    
    // Initialize with fixed seed for reproducibility
    var sim = try Simulation.init(allocator, 0xDEADBEEF);
    defer sim.deinit();
    
    // Add nodes
    const node1 = try sim.add_node();
    const node2 = try sim.add_node();
    
    // Script scenario
    sim.tick_multiple(10);
    sim.partition_nodes(node1, node2);
    sim.tick_multiple(50);
    sim.heal_partition(node1, node2);
    sim.tick_multiple(30);
    
    // Assert final state
    const state = try sim.get_node_filesystem_state(node1);
    defer /* cleanup state */;
    
    // Verify deterministic results
    try std.testing.expect(state.len > 0);
}
```

**Simulation Capabilities:**
- Network partitions with `partition_nodes()` / `heal_partition()`
- Packet loss with `set_packet_loss()`
- Network latency with `set_latency()`
- Time-controlled filesystem operations
- Deterministic PRNG for reproducible results

## Contributing

### Code Review Checklist

Before submitting PR:
- [ ] Code follows `docs/STYLE.md`
- [ ] All tests pass
- [ ] Tidy checks pass
- [ ] No debug prints in library code
- [ ] Documentation updated
- [ ] Benchmarks considered
- [ ] Simulation tests added if applicable

### Commit Message Format

```
type(scope): subject

Longer description if needed.

- List specific changes
- Include breaking change notes
- Reference issues if applicable
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

### Pull Request Process

1. **Create descriptive PR title**
   ```
   feat(query): implement semantic search with vector embeddings
   ```

2. **Fill out PR template**
   - Description of changes
   - Testing performed
   - Performance impact
   - Breaking changes

3. **Ensure CI passes**
   - All tests pass
   - Code quality checks pass
   - No security warnings

4. **Request review**
   - Tag relevant maintainers
   - Respond to feedback promptly
   - Update documentation as needed

## Debugging

### Common Issues

**Build failures:**
```bash
# Clean build artifacts
rm -rf zig-cache zig-out

# Update Zig if needed
./zig/download.sh
```

**Test failures:**
```bash
# Run with verbose output
zig build test -- --summary all

# Run single test
zig test src/failing_module.zig
```

**Tidy violations:**
```bash
# See specific violations
zig run src/tidy.zig

# Fix formatting
zig fmt .

# Check naming conventions in STYLE.md
```

### Debug Builds

```bash
# Debug build with assertions
zig build -Doptimize=Debug

# Release build for performance testing
zig build -Doptimize=ReleaseFast
```

### Profiling

```bash
# Build with profiling
zig build -Doptimize=ReleaseFast

# Profile with perf (Linux)
perf record ./zig-out/bin/benchmark all
perf report

# Profile with Instruments (macOS)
instruments -t "Time Profiler" ./zig-out/bin/benchmark all
```

## Architecture Overview

### Core Components

- **Query Engine**: Semantic and structural query processing
- **Block Storage**: Immutable block storage with content addressing
- **Simulation**: Deterministic testing framework
- **Shell**: Git integration and command execution

### Design Principles

- **Explicit over clever**: Code should be immediately readable
- **Performance aware**: Consider allocations and hot paths
- **Defensive programming**: Assert invariants liberally
- **Deterministic testing**: All code must work in simulation

### Performance Targets

- Block validation: < 1ms per block
- Query processing: < 10ms for typical queries
- Memory usage: < 100MB for 1M blocks
- Zero allocations in hot paths

## Resources

- [Zig Language Reference](https://ziglang.org/documentation/master/)
- [TigerBeetle Style Guide](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)
- [A Philosophy of Software Design](https://web.stanford.edu/~ouster/cgi-bin/book.php)
- [CortexDB Design Document](docs/DESIGN.md)

## Getting Help

- Check existing issues and discussions
- Review documentation in `docs/`
- Ask questions in development chat
- Create detailed bug reports with reproduction steps

Remember: CortexDB aims for production-grade quality. Every decision should be justified by RFC compliance, performance measurement, maintainability improvement, or user-facing benefit.
