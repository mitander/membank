# Development Guide

This guide covers the philosophy, workflow, and tools for contributing to KausalDB. We prioritize correctness, performance, and a streamlined developer experience.

## 1. Core Philosophy: Simulation-First Testing

Our testing philosophy is the foundation of KausalDB's reliability. We do not mock. We run the **exact same production code** inside a deterministic simulation harness that models hostile conditions.

- **Virtual File System (VFS):** All storage operations are written against a VFS abstraction (`src/vfs.zig`). In production, this VFS uses the real filesystem. In tests, it uses an in-memory, deterministic `SimulationVFS` (`src/simulation_vfs.zig`).
- **Deterministic Simulation:** The simulation framework (`src/simulation.zig`) controls time, I/O, and networking. Our test suites use this to validate behavior against disk corruption, I/O failures, network partitions, and power loss in a byte-for-byte reproducible manner.

When you add a new feature, your first thought should be: _How do I test this under hostile conditions in the simulation?_

## 2. Initial Setup

First, clone the repository. Then, install the project-specific Zig toolchain and set up the Git hooks. The hooks are **not optional**—they are a critical part of the workflow.

```bash
# Install the exact Zig version required by the project
./scripts/install_zig.sh

# Install pre-commit and pre-push hooks
./scripts/setup_hooks.sh
```

## 3. Development Workflow

### Code Quality and Validation

The Git hooks automate our quality gates.

1.  **`pre-commit`**: Before every commit, this hook runs:
    - **Formatter:** `zig fmt`
    - **Tidy Check:** `zig build tidy` (checks for architectural and style violations)
    - **Fast Tests:** `zig build test` (runs essential unit and integration tests)
2.  **`pre-push`**: Before you push, this hook runs a comprehensive local CI pipeline (`scripts/local_ci.sh`) that mirrors the GitHub Actions workflow. It performs cross-platform compilation checks, runs critical integration tests, and checks for performance regressions. This prevents most CI failures.

### Build & Test Commands

The `build.zig` file defines several targets for testing and validation.

- **Fast Developer Loop:** For day-to-day development. Runs in seconds.
  ```bash
  ./zig/zig build test
  ```
- **Full CI Check:** Runs the complete test suite, including stress and fault-injection tests. Use this before pushing.
  ```bash
  ./zig/zig build ci
  # Or run the script directly for more options
  ./scripts/local_ci.sh
  ```
- **Performance Regression Detection:** Runs benchmarks and compares them against the committed baseline to detect performance regressions.
  ```bash
  ./scripts/check_regression.sh
  ```
- **Fuzz Testing:** Runs the fuzzer to find bugs with random inputs. Several profiles are available.

  ```bash
  # Quick 5-minute fuzz for local validation
  ./scripts/fuzz.sh quick storage

  # Run continuously until stopped
  ./scripts/fuzz.sh continuous all
  ```

## 4. Debugging Workflow

We follow a tiered approach to debugging, starting with the fastest and simplest tools.

**Tier 1: Find the Source with GPA Safety**

The fastest way to find the origin of memory corruption is to enable the safety features of `std.heap.GeneralPurposeAllocator`. In the failing test, swap `std.testing.allocator` with a safety-enabled GPA:

```zig
// In your test file
var gpa = std.heap.GeneralPurposeAllocator(.{ .safety = true }){};
defer _ = gpa.deinit();
const allocator = gpa.allocator();
```

This will often pinpoint the exact location of a buffer overflow or use-after-free error.

**Tier 2: Deeper Analysis with Sanitizers**

If the GPA doesn't reveal the issue, use LLVM's sanitizers for a more detailed report. Our `test-sanitizer` build target has this pre-configured.

```bash
./zig/zig build test-sanitizer
```

This will provide detailed stack traces for memory errors.

**Tier 3: Interactive Inspection**

If the "what" is still unclear, use a debugger like LLDB or GDB to set breakpoints and inspect the program state interactively.

## 5. Testing Infrastructure

### Migrated Test Patterns

The project uses a unified testing infrastructure that provides:

- **QueryHarness**: Coordinated setup for query engine and storage engine testing
- **StorageHarness**: Dedicated storage subsystem testing with VFS integration
- **NetworkHarness**: Server and protocol testing with deterministic I/O
- **FaultInjectionHarness**: Systematic failure simulation and recovery testing

### Test Organization

Tests are organized by functionality and complexity:

- **Unit Tests** (`tests/unit/`): Fast, isolated component tests
- **Integration Tests** (`tests/integration/`): Cross-component interaction validation
- **Performance Tests** (`tests/performance/`): Benchmark and regression detection
- **Fault Injection** (`tests/fault_injection/`): Hostile condition simulation
- **Recovery Tests** (`tests/recovery/`): Data corruption and recovery scenarios

### Writing Tests

When adding new features:

1.  **Use appropriate harness**: Select QueryHarness, StorageHarness, or NetworkHarness based on subsystem
2.  **Test failure modes**: Every success path requires corresponding failure mode tests
3.  **Validate memory safety**: Follow arena-per-subsystem memory management with proper resource cleanup
4.  **Include performance tests**: Add benchmark coverage for performance-critical paths

### Performance Regression Testing

The `check_regression.sh` script provides automated regression detection:

- **Baseline Management**: `./scripts/check_regression.sh baseline` to update performance baseline
- **CI Integration**: Automatic regression checks with 15% latency and 10% throughput thresholds
- **Local Validation**: Run before commits to catch performance issues early
