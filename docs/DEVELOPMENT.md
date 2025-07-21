# CortexDB Development Guide

## 1. Development Philosophy

Welcome to CortexDB. We are building a high-performance, mission-critical database, and our development practices reflect that. Our philosophy is simple:

- **Simplicity is Non-Negotiable:** We build reliable systems by relentlessly pursuing simplicity. We choose simple, explicit patterns over complex, "magical" abstractions.
- **We Don't Mock; We Simulate:** The only way to trust a complex system is to test it holistically. Our primary method of validation is through a deterministic simulation framework that can reproduce the most hostile production environments byte-for-byte.
- **The Toolchain Serves the Developer:** Our build system, tests, and scripts are designed to be simple, consistent, and cross-platform. There should be zero friction in writing, testing, and debugging high-quality code.

## 2. One-Step Setup

Our goal is a "clone-and-build" developer experience on macOS, Linux, and Windows.

### Step 1: Install Project-Specific Zig

CortexDB depends on a specific version of the Zig toolchain. We provide a script to download it into a local `./zig/` directory. This ensures every developer and the CI environment uses the exact same compiler version, eliminating "works on my machine" issues.

```bash
# This will download the correct Zig version for your OS/architecture.
./scripts/install-zig.sh
```

### Step 2: Install Git Hooks

We enforce code quality and commit message standards automatically. This script installs the necessary pre-commit and commit-msg hooks into your local `.git` directory.

```bash
# This script is idempotent and safe to run multiple times.
./scripts/setup-hooks.sh
```

That's it. You are now ready to build CortexDB.

_(Optional)_: If you use `direnv`, running `direnv allow` will show a reminder to use the project's `./zig/zig` executable.

## 3. The Core Workflow: The Inner Loop

We have one command that represents the "inner loop" for 95% of development. It builds the project, runs all tests (unit, simulation, and integration), and verifies all code quality and formatting standards.

**This is the only command you need to run before committing:**

```bash
./cortex test
```

A successful run of this command means your code is correct, well-formatted, and meets our style guidelines. If it passes, you can commit with confidence.

## 4. The Toolchain: A Deeper Dive

While `./zig/zig build test` is your primary tool, the build system provides several granular targets for specific tasks.

### Main Commands

- **Run CortexDB:** `./zig/zig build run`
  Builds and runs the CortexDB database server.
- **Run All Tests:** `./zig/zig build test`
  As mentioned, this is the canonical way to validate your changes.
- **Quick Check:** `./zig/zig build check`
  Run tests and code quality checks together.
- **Performance Benchmarks:** `./zig/zig build benchmark`
  Build and run performance benchmarks.
- **Fuzz Testing:** `./zig/zig build fuzz`
  Build and run fuzz tests.
- **Code Quality:** `./zig/zig build tidy`
  Run CortexDB-specific linting and style checks.
- **Complete CI:** `./zig/zig build ci`
  Run the complete CI pipeline (formatting, tidy, tests).

### Test Categories

- **Unit Tests:** `./zig/zig build unit-test`
  Core module tests.
- **Simulation Tests:** `./zig/zig build simulation`
  Deterministic simulation framework tests.
- **Storage Tests:** `./zig/zig build storage_simulation`
  Storage layer simulation tests.
- **WAL Recovery Tests:** `./zig/zig build wal_recovery`
  Write-Ahead Log recovery tests.
- **WAL Memory Safety:** `./zig/zig build wal_memory_safety`
  WAL memory safety validation.
- **Memory Isolation Tests:** `./zig/zig build memory_isolation`
  Memory isolation and arena safety tests.
- **Integration Tests:** `./zig/zig build integration`
  End-to-end integration tests.

### Code Formatting

- **Check Formatting:** `./zig/zig build fmt`
  Verify code formatting without making changes.
- **Fix Formatting:** `./zig/zig build fmt-fix`
  Automatically fix code formatting issues.

### Setup Commands

- **Install Zig:** `./scripts/install-zig.sh`
  Install or update the project Zig toolchain.
- **Setup Hooks:** `./scripts/setup-hooks.sh`
  Setup git hooks for development.

## 5. Debugging Memory Issues: A Pragmatic Guide

We follow a tiered approach to debugging, escalating from simple checks to powerful tools.

### Tier 1: `GeneralPurposeAllocator` with Safety Checks

This is your first and most effective tool for finding memory corruption. If a test is crashing unpredictably, modify it to use a `GeneralPurposeAllocator` with `.safety = true`.

```zig
test "my component is crashing" {
    // Before: const allocator = std.testing.allocator;

    // The fix: Create a safety-enabled GPA for this test.
    var gpa = std.heap.GeneralPurposeAllocator(.{.safety = true}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // ... rest of the test code ...
}
```

This will instantly detect most use-after-free, double-free, and buffer overflow bugs, causing a panic at the _exact line_ of the corruption. This turns most debugging sessions into a 5-minute fix.

### Tier 2: LLVM AddressSanitizer (ASan)

For more subtle bugs, we use the AddressSanitizer built into the LLVM toolchain.

```bash
# Run the entire test suite with ASan enabled.
./zig/zig build test -fsanitize-address
```

ASan will halt the program on any memory error and provide a detailed report, including the stack trace of the invalid access, the allocation site, and the deallocation site.

## 6. Build Modes and Optimization Levels

CortexDB supports all Zig optimization levels, but we recommend specific modes for different use cases:

### Recommended Build Modes

- **Development/Testing:** `./zig/zig build test -Doptimize=ReleaseSafe`
- **CI/Automation:** `-Doptimize=ReleaseSafe` (default in our CI pipeline)
- **Production Releases:** `-Doptimize=ReleaseFast`
- **Size-Constrained Deployments:** `-Doptimize=ReleaseSmall`

### Debug Mode Limitations

**Important:** Debug mode (`-Doptimize=Debug`) has known linking performance issues:

- **Symptom:** Linking phase hangs for 60+ seconds during compilation
- **Root Cause:** Zig compiler performance issue with large debug symbol generation
- **Workaround:** Use `ReleaseSafe` for most development work

**Why ReleaseSafe is Better for Development:**

- Includes all safety checks (bounds checking, integer overflow detection)
- Fast compilation and linking (< 10 seconds)
- Catches the same classes of bugs as Debug mode
- Used in our CI pipeline for consistent validation

**When to Use Debug Mode:**

- Only when you specifically need full debug symbols for external debuggers
- Be prepared for slow linking times
- Consider using `ReleaseSafe` with Tier 1/Tier 2 debugging tools instead

### CI Configuration

Our CI pipeline uses `ReleaseSafe` by default to ensure:

- Fast, reliable builds (< 5 minute test cycles)
- Comprehensive safety checking without Debug mode overhead
- Consistent behavior between local development and CI validation

## 7. Contributing

### Commit Messages

We enforce the **Conventional Commits** standard via a `commit-msg` hook. This allows for automated changelog generation and clear project history. Your commit messages must follow this pattern.

**The Anatomy of a Great CortexDB Commit:**

A great commit has a good message, and a description includign a short description and bullet points of relevant changes.

```
    feat(ci): add automated performance regression detection

    Add CI pipeline to detect performance regressions automatically.

    - Add JSON output format to benchmark framework (--json flag)
    - Create performance-ci.sh script with 15% slowdown threshold
    - Integrate performance job into GitHub Actions workflow
    - Store baseline in .github/performance-baseline.json
    - Shell-based detection using jq/awk (no Python dependency)
    - Upload performance artifacts and comment on PRs
    - Fail CI when operations are >15% slower than baseline
```

### Pull Requests

1.  **Title:** Use a conventional commit title (e.g., `feat(query): add metadata filtering`).
2.  **Description:** Clearly explain the "why" behind your change. Link to any relevant issues.
3.  **Checklist:** Ensure `zig build test` passes locally before submitting.
4.  **CI:** Ensure all automated checks pass on your PR.
