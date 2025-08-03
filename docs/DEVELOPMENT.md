# Kausal: Development & Testing

This document provides the necessary protocols for building, testing, and debugging the Kausal system.

## 1. Initial Setup

These commands must be run once after cloning the repository. They establish the correct toolchain and local quality gates.

```bash
# Install the project-specific Zig version
./scripts/install_zig.sh

# Install pre-commit and commit-msg git hooks
./scripts/setup_hooks.sh
```

## 2. Core Build Commands

The build system provides several targets for different phases of development.

*   **Fast Iteration Cycle:** Runs fast unit tests. Use this during active development.
    ```bash
    ./zig/zig build test
    ```
*   **Full Validation Suite:** Runs the complete test suite, including stress and simulation tests. This mirrors the CI environment.
    ```bash
    ./zig/zig build test-all
    ```
*   **Execute the Server:** Builds and runs the main server binary.
    ```bash
    ./zig/zig build run
    ```

## 3. Debugging Protocol

Follow this tiered protocol to diagnose failures. Do not deviate.

*   **Tier 1: Isolate Corruption Source.** In the failing test, replace `std.testing.allocator` with `std.heap.GeneralPurposeAllocator(.{.safety = true})`. This is the fastest method to identify the origin of memory corruption (e.g., use-after-free, buffer overflow).
*   **Tier 2: Detailed Analysis.** If the origin is unclear, run the tests with the LLVM AddressSanitizer. This provides a detailed report of the memory error with full stack traces.
    ```bash
    ./zig/zig build test -fsanitize-address
    ```
*   **Tier 3: Interactive Inspection.** Only if the failure is logical and not memory-related, use LLDB to set breakpoints and inspect the program state.

## 4. Toolchain

The project uses several tools to automate quality enforcement.

*   **Git Hooks:**
    *   `pre-commit`: Runs `zig fmt`, `zig build tidy`, and fast unit tests. Rejects commits that fail quality checks.
    *   `commit-msg`: Enforces the Conventional Commits format for all commit messages.
*   **CI/CD (`.github/workflows/ci.yml`):** The CI pipeline automates validation. Key jobs include:
    *   `test`: Runs the full test suite.
    *   `build-matrix`: Compiles the project on Ubuntu and macOS.
    - `performance`: Detects performance regressions against a committed baseline.
    - `memory-safety`: Runs the test suite under Valgrind to detect memory leaks and errors.
*   **Fuzz Testing (`scripts/fuzz.sh`):** A fuzz testing harness is used to discover bugs by feeding random and malformed data to the system. It supports several profiles (`quick`, `ci`, `deep`, `continuous`).
