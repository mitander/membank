# Development Guide

This guide covers the philosophy, workflow, and tools for contributing to KausalDB. We prioritize correctness, performance, and a streamlined developer experience.

## 1. Core Philosophy: Simulation-First Testing

Our testing philosophy is the foundation of KausalDB's reliability. We do not mock. We run the **exact same production code** inside a deterministic simulation harness that models hostile conditions.

-   **Virtual File System (VFS):** All storage operations are written against a VFS abstraction (`src/core/vfs.zig`). In production, this VFS uses the real filesystem. In tests, it uses an in-memory, deterministic `SimulationVFS` (`src/sim/simulation_vfs.zig`).
-   **Deterministic Simulation:** The simulation framework (`src/sim/simulation.zig`) controls time, I/O, and networking. Our test suites (`tests/fault_injection/`) use this to validate behavior against disk corruption, I/O failures, network partitions, and power loss in a byte-for-byte reproducible manner.

When you add a new feature, your first thought should be: *How do I test this under hostile conditions in the simulation?*

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
    *   **Formatter:** `zig fmt`
    *   **Tidy Check:** `zig build tidy` (checks for architectural and style violations)
    *   **Fast Tests:** `zig build test` (runs essential unit and integration tests)
2.  **`pre-push`**: Before you push, this hook runs a comprehensive local CI pipeline (`scripts/local_ci.sh`) that mirrors the GitHub Actions workflow. It performs cross-platform compilation checks, runs critical integration tests, and checks for performance regressions. This prevents most CI failures.

### Build & Test Commands

The `build.zig` file defines several targets for testing and validation.

-   **Fast Developer Loop:** For day-to-day development. Runs in seconds.
    ```bash
    ./zig/zig build test
    ```
-   **Full CI Check:** Runs the complete test suite, including stress and fault-injection tests. Use this before pushing.
    ```bash
    ./zig/zig build ci
    # Or run the script directly for more options
    ./scripts/local_ci.sh
    ```
-   **Performance Benchmarking:** Runs benchmarks and compares them against the committed baseline (`.github/performance-baseline.json`).
    ```bash
    ./scripts/benchmark.sh
    ```
-   **Fuzz Testing:** Runs the fuzzer to find bugs with random inputs. Several profiles are available.
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
