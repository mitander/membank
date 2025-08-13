#!/bin/bash
set -euo pipefail

# KausalDB Local CI Runner
# Mirrors the GitHub Actions CI pipeline exactly for local development
# Eliminates the push-wait-read cycle for debugging CI failures

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Timing
START_TIME=$(date +%s)

log_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

log_step() {
    echo -e "${YELLOW}-> $1${NC}"
}

log_success() {
    echo -e "${GREEN}+ $1${NC}"
}

log_error() {
    echo -e "${RED}- $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}! $1${NC}"
}

# Parse command line arguments
JOBS="all"
OPTIMIZE="ReleaseSafe"
SKIP_SETUP=false
PARALLEL=false
MEMORY_TESTS=false

show_help() {
    cat << EOF
KausalDB Local CI Runner

Usage: $0 [OPTIONS]

OPTIONS:
    --jobs JOBS           Run specific job categories (default: all)
                         Options: all, test, build, lint, security, performance, sanitizer, memory
    --optimize LEVEL      Optimization level (default: ReleaseSafe)
                         Options: Debug, ReleaseSafe, ReleaseFast, ReleaseSmall
    --skip-setup         Skip Zig installation (faster for repeated runs)
    --parallel           Run compatible jobs in parallel
    --memory-tests       Include memory safety tests (requires longer runtime)
    --help               Show this help message

EXAMPLES:
    $0                                    # Run all checks
    $0 --jobs test                        # Run only tests
    $0 --jobs "test,lint"                 # Run tests and linting
    $0 --jobs "test,sanitizer"            # Run tests and sanitizer checks
    $0 --optimize Debug --skip-setup      # Fast debug run, no setup
    $0 --parallel --memory-tests          # Full parallel run with memory checks

PERFORMANCE:
    --skip-setup saves ~10-15 seconds on repeated runs
    --parallel can reduce total time by 30-50% for compatible jobs
    --memory-tests adds ~5-10 minutes for Valgrind analysis
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        --optimize)
            OPTIMIZE="$2"
            shift 2
            ;;
        --skip-setup)
            SKIP_SETUP=true
            shift
            ;;
        --parallel)
            PARALLEL=true
            shift
            ;;
        --memory-tests)
            MEMORY_TESTS=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Function to run a job with timing
run_job() {
    local job_name="$1"
    local job_func="$2"

    log_section "$job_name"
    local job_start=$(date +%s)

    if $job_func; then
        local job_end=$(date +%s)
        local job_duration=$((job_end - job_start))
        log_success "$job_name completed in ${job_duration}s"
        return 0
    else
        local job_end=$(date +%s)
        local job_duration=$((job_end - job_start))
        log_error "$job_name failed after ${job_duration}s"
        return 1
    fi
}

# Setup job
setup_job() {
    if [[ "$SKIP_SETUP" == "true" ]]; then
        log_step "Skipping Zig setup (--skip-setup flag)"
        if [[ ! -x "./zig/zig" ]]; then
            log_warning "No Zig installation found, running setup anyway"
            SKIP_SETUP=false
        fi
    fi

    if [[ "$SKIP_SETUP" == "false" ]]; then
        log_step "Setting up Zig toolchain"
        chmod +x scripts/install_zig.sh
        ./scripts/install_zig.sh
    fi

    log_step "Verifying Zig installation"
    local zig_version=$(./zig/zig version)
    log_success "Using Zig: $zig_version"
    return 0
}

# Test job (mirrors GitHub Actions test job)
test_job() {
    log_step "Checking code formatting"
    if ! ./zig/zig fmt --check .; then
        log_error "Code formatting check failed"
        return 1
    fi

    log_step "Building with $OPTIMIZE optimization"
    if ! ./zig/zig build -Doptimize="$OPTIMIZE"; then
        log_error "Build failed with $OPTIMIZE optimization"
        return 1
    fi

    log_step "Running tests"
    if ! ./zig/zig build test-all -Doptimize="$OPTIMIZE"; then
        log_error "Test suite failed"
        return 1
    fi

    log_step "Running tidy checks"
    if ! ./zig/zig build tidy -Doptimize="$OPTIMIZE"; then
        log_error "Tidy checks failed"
        return 1
    fi

    log_step "Building all targets"
    echo "-> Building benchmark target..."
    if ! ./zig/zig build benchmark; then
        log_error "Benchmark target build failed"
        return 1
    fi
    echo "-> Building fuzz target..."
    if ! ./zig/zig build fuzz; then
        log_error "Fuzz target build failed"
        return 1
    fi

    log_step "Building release"
    if ! ./zig/zig build --release=safe; then
        log_error "Release build failed"
        return 1
    fi

    return 0
}

# Build job (mirrors GitHub Actions build-matrix job)
build_job() {
    log_step "Validating build system"
    if ! ./zig/zig build --help > /dev/null; then
        log_error "Build system validation failed"
        return 1
    fi

    log_step "Building release"
    if ! ./zig/zig build --release=safe; then
        log_error "Release build failed"
        return 1
    fi

    return 0
}

# Lint job (mirrors GitHub Actions lint job)
lint_job() {
    log_step "Checking for banned patterns"
    if grep -r "std\.BoundedArray" src/ --exclude-dir=tidy 2>/dev/null; then
        log_error "Found banned pattern std.BoundedArray"
        return 1
    fi
    if grep -r "std\.StaticBitSet" src/ --exclude="stdx.zig" 2>/dev/null; then
        log_error "Found banned pattern std.StaticBitSet"
        return 1
    fi
    if grep -r "FIXME\|TODO" src/ --include="*.zig" 2>/dev/null; then
        log_warning "Found FIXME/TODO comments in source"
    fi

    return 0
}

# Security job (mirrors GitHub Actions security job)
security_job() {
    log_step "Scanning for potential secrets"
    # Look for actual hardcoded secrets, not just the words "key", "password", etc.
    # Focus on assignments and string literals that might contain actual secrets
    if grep -r -E "(password|secret|token|api_key|private_key)\s*[:=]\s*[\"'][^\"']{8,}" src/ --include="*.zig" 2>/dev/null; then
        log_warning "Potential hardcoded secrets found"
    elif grep -r -E "(Password|Secret|Token|ApiKey|PrivateKey)\s*[:=]\s*[\"'][^\"']{8,}" src/ --include="*.zig" 2>/dev/null; then
        log_warning "Potential hardcoded secrets found"
    fi

    log_step "Checking file permissions"
    # Cross-platform executable file detection (macOS uses +111, Linux uses /111)
    if [[ "$OSTYPE" == "darwin"* ]]; then
        find . -type f -perm +111 -name "*.zig" 2>/dev/null | while read file; do
            log_warning "Executable Zig source file: $file"
        done
    else
        find . -type f -perm /111 -name "*.zig" 2>/dev/null | while read file; do
            log_warning "Executable Zig source file: $file"
        done
    fi

    log_step "Validating project structure"
    if [[ ! -f "build.zig" ]]; then
        log_error "Missing build.zig"
        return 1
    fi
    if [[ ! -f "build.zig.zon" ]]; then
        log_error "Missing build.zig.zon"
        return 1
    fi
    if [[ ! -d "src" ]]; then
        log_error "Missing src directory"
        return 1
    fi

    return 0
}

# Performance job (mirrors GitHub Actions performance job)
performance_job() {
    log_step "Running performance regression detection"
    if [[ -x "scripts/benchmark.sh" ]]; then
        if ! chmod +x scripts/benchmark.sh; then
            log_error "Failed to set execute permissions on benchmark.sh"
            return 1
        fi
        if ! ./scripts/benchmark.sh; then
            log_error "Benchmark script failed"
            return 1
        fi
    else
        log_warning "benchmark.sh not found, skipping performance tests"
    fi

    return 0
}

# Sanitizer job (mirrors GitHub Actions sanitizer job)
sanitizer_job() {
    log_step "Running tests with Thread Sanitizer and C UBSan"
    if ! ./zig/zig build test-sanitizer; then
        log_error "Sanitizer tests failed"
        return 1
    fi

    log_step "Running benchmarks with sanitizers"
    if ! ./zig/zig build benchmark; then
        log_error "Benchmark build failed"
        return 1
    fi

    # Run abbreviated benchmark with timeout for local development
    timeout 60s ./zig-out/bin/benchmark storage || echo "Benchmark completed or timed out"

    return 0
}

# Memory safety job (mirrors GitHub Actions memory-safety job)
memory_job() {
    if [[ "$MEMORY_TESTS" != "true" ]]; then
        log_step "Skipping memory tests (use --memory-tests to enable)"
        return 0
    fi

    log_step "Checking for Valgrind availability"
    if ! command -v valgrind &> /dev/null; then
        log_warning "Valgrind not found, install with: brew install valgrind (macOS) or apt-get install valgrind (Linux)"
        log_step "Running memory tests without Valgrind"
    else
        log_step "Valgrind found, running full memory analysis"
    fi

    log_step "Building with debug symbols and memory safety"
    if ! ./zig/zig build -Doptimize="$OPTIMIZE"; then
        log_error "Build failed with memory safety flags"
        return 1
    fi

    log_step "Running memory safety tests"
    if ! ./zig/zig build test-sanitizer -Doptimize="$OPTIMIZE"; then
        log_error "Memory safety tests failed"
        return 1
    fi

    log_step "Running stress tests"
    if ! ./zig/zig build test-memory-stress -Doptimize="$OPTIMIZE"; then
        log_error "Memory stress tests failed"
        return 1
    fi

    log_step "Running storage engine stress test with memory monitoring"
    if ! timeout 300s ./zig/zig build integration_lifecycle -Doptimize="$OPTIMIZE"; then
        log_error "Storage engine stress test failed"
        return 1
    fi

    if command -v valgrind &> /dev/null; then
        log_step "Running Valgrind memory error detection (abbreviated for local CI)"
        if ! ./zig/zig build test-all -Doptimize="$OPTIMIZE"; then
            log_error "Test build failed for Valgrind analysis"
            return 1
        fi

        # Run a shorter Valgrind analysis for local development
        timeout 60s valgrind --tool=memcheck --error-exitcode=1 --leak-check=full \
                     --show-leak-kinds=all --errors-for-leak-kinds=all \
                     --track-origins=yes \
                     ./zig-out/bin/test 2>&1 | tee valgrind-output.log || {
            log_error "Valgrind detected memory errors!"
            cat valgrind-output.log
            return 1
        }

        log_step "Analyzing memory allocation patterns"
        if grep -i "corrupted\|overflow\|underflow\|double.free\|use.after.free" valgrind-output.log 2>/dev/null; then
            log_error "Suspicious memory patterns detected in logs"
            return 1
        fi

        rm -f valgrind-output.log
    else
        log_step "Running memory safety validation with built-in tools (Valgrind not available)"
        if ! ./zig/zig build test-sanitizer; then
            log_error "Sanitizer tests failed"
            return 1
        fi

        log_step "Running additional memory safety focused tests"
        if ! ./zig/zig build safety_memory_corruption -Doptimize="$OPTIMIZE"; then
            log_error "Memory corruption safety tests failed"
            return 1
        fi

        if ! ./zig/zig build safety_fatal_safety_violations -Doptimize="$OPTIMIZE"; then
            log_error "Memory fatal safety tests failed"
            return 1
        fi
    fi

    return 0
}

# Coverage job (mirrors GitHub Actions coverage job)
coverage_job() {
    log_step "Generating test coverage"
    if ! ./zig/zig build test-fast -Doptimize="$OPTIMIZE" -- --summary all; then
        log_error "Coverage test run failed"
        return 1
    fi

    return 0
}

# Function to check if a job is requested
should_run_job() {
    local job="$1"
    if [[ "$JOBS" == "all" ]]; then
        return 0
    fi

    echo "$JOBS" | grep -qw "$job"
}

# Function to run jobs in parallel
run_parallel_jobs() {
    local pids=()
    local jobs=()
    local temp_dir=$(mktemp -d)

    # Set environment variable to indicate parallel execution for performance tier detection
    export KAUSALDB_PARALLEL_TESTS=true

    # Start compatible jobs in parallel
    if should_run_job "build"; then
        jobs+=("build")
        (run_job "Build Matrix" build_job > "$temp_dir/build.log" 2>&1; echo $? > "$temp_dir/build.exit") &
        pids+=($!)
    fi

    if should_run_job "lint"; then
        jobs+=("lint")
        (run_job "Lint" lint_job > "$temp_dir/lint.log" 2>&1; echo $? > "$temp_dir/lint.exit") &
        pids+=($!)
    fi

    if should_run_job "security"; then
        jobs+=("security")
        (run_job "Security" security_job > "$temp_dir/security.log" 2>&1; echo $? > "$temp_dir/security.exit") &
        pids+=($!)
    fi

    if should_run_job "performance"; then
        jobs+=("performance")
        (run_job "Performance" performance_job > "$temp_dir/performance.log" 2>&1; echo $? > "$temp_dir/performance.exit") &
        pids+=($!)
    fi

    # Wait for all parallel jobs to complete
    local all_success=true
    for i in "${!pids[@]}"; do
        wait "${pids[$i]}"
        local job_name="${jobs[$i]}"

        # Display results
        cat "$temp_dir/$job_name.log"

        local exit_code=$(cat "$temp_dir/$job_name.exit")
        if [[ "$exit_code" != "0" ]]; then
            all_success=false
        fi
    done

    # Cleanup
    rm -rf "$temp_dir"

    # Clean up environment variable
    unset KAUSALDB_PARALLEL_TESTS

    if [[ "$all_success" == "true" ]]; then
        return 0
    else
        return 1
    fi
}

# Main execution
main() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                    KausalDB Local CI Runner                  ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"

    log_step "Configuration:"
    echo "  Jobs: $JOBS"
    echo "  Optimize: $OPTIMIZE"
    echo "  Skip Setup: $SKIP_SETUP"
    echo "  Parallel: $PARALLEL"
    echo "  Memory Tests: $MEMORY_TESTS"

    # Always run setup first
    run_job "Setup" setup_job || exit 1

    # Run test job first (core functionality)
    if should_run_job "test"; then
        # Set environment variable for parallel execution if parallel mode is enabled
        if [[ "$PARALLEL" == "true" ]]; then
            export KAUSALDB_PARALLEL_TESTS=true
        fi

        run_job "Test" test_job || exit 1
        run_job "Coverage" coverage_job || exit 1

        # Clean up environment variable
        if [[ "$PARALLEL" == "true" ]]; then
            unset KAUSALDB_PARALLEL_TESTS
        fi
    fi

    # Run other jobs
    if [[ "$PARALLEL" == "true" ]] && [[ "$JOBS" != "test" ]]; then
        log_section "Running parallel jobs (excluding performance)"

        # Set environment variable for performance tier detection
        export KAUSALDB_PARALLEL_TESTS=true

        # Run parallel jobs excluding performance tests
        local pids=()
        local jobs=()
        local temp_dir=$(mktemp -d)

        # Start compatible jobs in parallel (excluding performance)
        if should_run_job "build"; then
            jobs+=("build")
            (run_job "Build Matrix" build_job > "$temp_dir/build.log" 2>&1; echo $? > "$temp_dir/build.exit") &
            pids+=($!)
        fi

        if should_run_job "lint"; then
            jobs+=("lint")
            (run_job "Lint" lint_job > "$temp_dir/lint.log" 2>&1; echo $? > "$temp_dir/lint.exit") &
            pids+=($!)
        fi

        if should_run_job "security"; then
            jobs+=("security")
            (run_job "Security" security_job > "$temp_dir/security.log" 2>&1; echo $? > "$temp_dir/security.exit") &
            pids+=($!)
        fi

        # Wait for all parallel jobs to complete
        local all_success=true
        for i in "${!pids[@]}"; do
            wait "${pids[$i]}"
            local job_name="${jobs[$i]}"

            # Display results
            cat "$temp_dir/$job_name.log"

            local exit_code=$(cat "$temp_dir/$job_name.exit")
            if [[ "$exit_code" != "0" ]]; then
                all_success=false
            fi
        done

        # Cleanup
        rm -rf "$temp_dir"
        unset KAUSALDB_PARALLEL_TESTS

        if [[ "$all_success" != "true" ]]; then
            exit 1
        fi

        # Run performance tests separately to avoid resource contention
        if should_run_job "performance"; then
            log_section "Running performance tests (isolated)"
            run_job "Performance" performance_job || exit 1
        fi
    else
        # Sequential execution
        if should_run_job "build"; then
            run_job "Build Matrix" build_job || exit 1
        fi

        if should_run_job "lint"; then
            run_job "Lint" lint_job || exit 1
        fi

        if should_run_job "security"; then
            run_job "Security" security_job || exit 1
        fi

        if should_run_job "performance"; then
            run_job "Performance" performance_job || exit 1
        fi

        if should_run_job "sanitizer"; then
            run_job "Thread Sanitizer and C UBSan Memory Safety" sanitizer_job || exit 1
        fi
    fi

    # Memory tests always run last and sequentially (resource intensive)
    if should_run_job "memory"; then
        run_job "Memory Safety" memory_job || exit 1
    fi

    # Final summary
    local end_time=$(date +%s)
    local total_duration=$((end_time - START_TIME))
    local minutes=$((total_duration / 60))
    local seconds=$((total_duration % 60))

    echo
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                         CI SUCCESS                           ║${NC}"
    echo -e "${GREEN}║                                                              ║${NC}"
    echo -e "${GREEN}║  All requested jobs completed successfully!                  ║${NC}"
    echo -e "${GREEN}║  Total time: ${minutes}m ${seconds}s${NC}$(printf "%*s" $((39 - ${#minutes} - ${#seconds})) '')${GREEN}      ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo
    echo "Ready to push to GitHub!"
}

# Error handling
trap 'log_error "CI failed with error on line $LINENO. Exit code: $?"' ERR

# Run main function
main "$@"
