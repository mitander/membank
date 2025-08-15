#!/bin/bash
#
# KausalDB Performance Benchmark Runner
#
# Executes performance benchmarks and validates against regression thresholds.
# This script is used by local CI and can be run standalone for performance analysis.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAUSALDB_ROOT="$(dirname "$SCRIPT_DIR")"
BENCHMARK_BINARY="$KAUSALDB_ROOT/zig-out/bin/benchmark"
RESULTS_DIR="$KAUSALDB_ROOT/benchmark_results"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

show_help() {
    cat << 'EOF'
KausalDB Performance Benchmark Runner

This script runs performance benchmarks and validates results against regression 
thresholds. It's designed for both local development and CI integration.

USAGE:
    ./scripts/benchmark.sh [OPTIONS] [BENCHMARK_TYPE]

BENCHMARK TYPES:
    all            Run all benchmarks (default)
    storage        Storage engine benchmarks only
    query          Query engine benchmarks only  
    quick          Fast benchmark subset for CI
    regression     Regression detection benchmarks

OPTIONS:
    --json         Output results in JSON format
    --save         Save results to benchmark_results/ directory
    --compare      Compare against previous results (requires --save)
    --threshold N  Regression threshold percentage (default: 20)
    --help         Show this help message

EXAMPLES:
    # Quick benchmark for local development
    ./scripts/benchmark.sh quick

    # Full benchmark suite with regression detection
    ./scripts/benchmark.sh --save --compare all

    # Storage benchmarks with JSON output
    ./scripts/benchmark.sh --json storage

    # CI-friendly quick benchmarks
    ./scripts/benchmark.sh --threshold 25 quick

PERFORMANCE THRESHOLDS:
    The script validates performance against established baselines:
    - Block writes: <100µs for 1MB+ blocks
    - Block reads: <50µs typical
    - Query operations: <10µs for single block lookup
    - WAL operations: <200µs for durability guarantees

For detailed analysis, see tests/performance/large_block_benchmark.zig
EOF
}

# Ensure bc is available for calculations
if ! command -v bc &> /dev/null; then
    log_error "bc calculator not found. Please install bc for performance calculations."
    exit 1
fi

# Build benchmark if needed
ensure_benchmark_binary() {
    if [[ ! -x "$BENCHMARK_BINARY" ]]; then
        log_step "Building benchmark binary"
        cd "$KAUSALDB_ROOT"
        if ! ./zig/zig build benchmark; then
            log_error "Failed to build benchmark binary"
            exit 1
        fi
    fi
}

# Parse benchmark output and extract performance metrics
parse_benchmark_output() {
    local output="$1"
    local benchmark_type="$2"
    
    # Extract timing information from benchmark output
    # Format: "Operation: 1234µs" or "Operation: 1.234ms"
    local timing=$(echo "$output" | grep -E "(µs|ms|ns)" | head -1)
    
    if [[ -n "$timing" ]]; then
        # Convert to microseconds for consistent comparison
        local value=$(echo "$timing" | grep -oE '[0-9]+\.?[0-9]*')
        local unit=$(echo "$timing" | grep -oE '(ns|µs|ms|s)')
        
        case "$unit" in
            "ns") echo "scale=0; $value / 1000" | bc -l ;;
            "µs") echo "$value" | cut -d'.' -f1 ;;
            "ms") echo "scale=0; $value * 1000" | bc -l ;;
            "s")  echo "scale=0; $value * 1000000" | bc -l ;;
            *) echo "0" ;;
        esac
    else
        echo "0"
    fi
}

# Run individual benchmark and validate performance
run_benchmark() {
    local benchmark_type="$1"
    local json_output="$2"
    local save_results="$3"
    
    log_step "Running $benchmark_type benchmark"
    
    local cmd="$BENCHMARK_BINARY"
    if [[ "$json_output" == "true" ]]; then
        cmd="$cmd --json"
    fi
    cmd="$cmd $benchmark_type"
    
    # Capture both stdout and timing
    local start_time=$(date +%s%N)
    local output
    if ! output=$($cmd 2>&1); then
        log_error "Benchmark $benchmark_type failed"
        echo "$output"
        return 1
    fi
    local end_time=$(date +%s%N)
    local wall_time_us=$(( (end_time - start_time) / 1000 ))
    
    echo "$output"
    
    # Extract performance metrics
    local perf_value=$(parse_benchmark_output "$output" "$benchmark_type")
    
    # Validate against thresholds
    local threshold=""
    case "$benchmark_type" in
        "block-write") threshold="100000" ;;
        "block-read") threshold="50000" ;;
        "single-query") threshold="10000" ;;
        "wal-flush") threshold="200000" ;;
    esac
    
    if [[ -n "$threshold" && "$perf_value" -gt 0 ]]; then
        if [[ "$perf_value" -gt "$threshold" ]]; then
            log_warning "$benchmark_type performance: ${perf_value}µs exceeds threshold ${threshold}µs"
        else
            log_info "$benchmark_type performance: ${perf_value}µs (within ${threshold}µs threshold)"
        fi
    fi
    
    # Save results if requested
    if [[ "$save_results" == "true" ]]; then
        mkdir -p "$RESULTS_DIR"
        local timestamp=$(date +"%Y%m%d_%H%M%S")
        local result_file="$RESULTS_DIR/${benchmark_type}_${timestamp}.txt"
        
        cat > "$result_file" << EOF
Benchmark: $benchmark_type
Timestamp: $(date)
Wall Time: ${wall_time_us}µs
Performance: ${perf_value}µs
Threshold: ${threshold:-N/A}µs
Status: $(if [[ -n "$threshold" ]] && [[ "$perf_value" -gt "$threshold" ]]; then echo "EXCEEDS_THRESHOLD"; else echo "OK"; fi)

Output:
$output
EOF
        log_info "Results saved to $result_file"
    fi
    
    return 0
}

# Run quick benchmark subset (for CI)
run_quick_benchmarks() {
    local json_output="$1"
    local save_results="$2"
    
    log_info "Running quick benchmark subset for CI"
    
    # Run most critical benchmarks only
    local benchmarks=("block-write" "block-read" "single-query")
    local all_passed=true
    
    for benchmark in "${benchmarks[@]}"; do
        if ! run_benchmark "$benchmark" "$json_output" "$save_results"; then
            all_passed=false
        fi
        echo # Add spacing between benchmarks
    done
    
    if [[ "$all_passed" == "true" ]]; then
        log_info "Quick benchmarks completed successfully"
        return 0
    else
        log_error "Some quick benchmarks failed"
        return 1
    fi
}

# Run storage benchmarks
run_storage_benchmarks() {
    local json_output="$1"
    local save_results="$2"
    
    log_info "Running storage engine benchmarks"
    
    local benchmarks=("block-write" "block-read" "block-update" "block-delete" "wal-flush")
    local all_passed=true
    
    for benchmark in "${benchmarks[@]}"; do
        if ! run_benchmark "$benchmark" "$json_output" "$save_results"; then
            all_passed=false
        fi
        echo
    done
    
    if [[ "$all_passed" == "true" ]]; then
        log_info "Storage benchmarks completed successfully"
        return 0
    else
        log_error "Some storage benchmarks failed"
        return 1
    fi
}

# Run query benchmarks
run_query_benchmarks() {
    local json_output="$1"
    local save_results="$2"
    
    log_info "Running query engine benchmarks"
    
    local benchmarks=("single-query" "batch-query")
    local all_passed=true
    
    for benchmark in "${benchmarks[@]}"; do
        if ! run_benchmark "$benchmark" "$json_output" "$save_results"; then
            all_passed=false
        fi
        echo
    done
    
    if [[ "$all_passed" == "true" ]]; then
        log_info "Query benchmarks completed successfully"
        return 0
    else
        log_error "Some query benchmarks failed"
        return 1
    fi
}

# Run all benchmarks
run_all_benchmarks() {
    local json_output="$1"
    local save_results="$2"
    
    log_info "Running complete benchmark suite"
    
    # Just use the benchmark binary's "all" command
    if ! run_benchmark "all" "$json_output" "$save_results"; then
        log_error "Complete benchmark suite failed"
        return 1
    fi
    
    log_info "Complete benchmark suite completed successfully"
    return 0
}

# Compare with previous results
compare_results() {
    local benchmark_type="$1"
    
    if [[ ! -d "$RESULTS_DIR" ]]; then
        log_warning "No previous results found for comparison"
        return 0
    fi
    
    local latest_results=$(find "$RESULTS_DIR" -name "${benchmark_type}_*.txt" -type f | sort | tail -2)
    local result_count=$(echo "$latest_results" | wc -l)
    
    if [[ "$result_count" -lt 2 ]]; then
        log_warning "Insufficient results for comparison (need at least 2 runs)"
        return 0
    fi
    
    local previous=$(echo "$latest_results" | head -1)
    local current=$(echo "$latest_results" | tail -1)
    
    local prev_perf=$(grep "Performance:" "$previous" | grep -oE '[0-9]+')
    local curr_perf=$(grep "Performance:" "$current" | grep -oE '[0-9]+')
    
    if [[ -n "$prev_perf" && -n "$curr_perf" ]]; then
        local change_percent=$(echo "scale=1; (($curr_perf - $prev_perf) * 100) / $prev_perf" | bc -l)
        
        if (( $(echo "$change_percent > 20" | bc -l) )); then
            log_warning "Performance regression detected: ${change_percent}% slower than previous run"
        elif (( $(echo "$change_percent < -20" | bc -l) )); then
            log_info "Performance improvement detected: ${change_percent}% faster than previous run"
        else
            log_info "Performance stable: ${change_percent}% change from previous run"
        fi
    fi
}

# Main execution
main() {
    local json_output=false
    local save_results=false
    local compare_results_flag=false
    local threshold=20
    local benchmark_type="all"
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --json)
                json_output=true
                shift
                ;;
            --save)
                save_results=true
                shift
                ;;
            --compare)
                compare_results_flag=true
                save_results=true  # comparison requires saved results
                shift
                ;;
            --threshold)
                threshold="$2"
                shift 2
                ;;
            --help)
                show_help
                exit 0
                ;;
            all|storage|query|quick|regression)
                benchmark_type="$1"
                shift
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    log_info "KausalDB Performance Benchmark Runner"
    log_info "====================================="
    log_info "Benchmark type: $benchmark_type"
    log_info "JSON output: $json_output"
    log_info "Save results: $save_results"
    echo
    
    # Ensure benchmark binary exists
    ensure_benchmark_binary
    
    # Run benchmarks based on type
    case "$benchmark_type" in
        quick)
            run_quick_benchmarks "$json_output" "$save_results"
            ;;
        storage)
            run_storage_benchmarks "$json_output" "$save_results"
            ;;
        query)
            run_query_benchmarks "$json_output" "$save_results"
            ;;
        all)
            run_all_benchmarks "$json_output" "$save_results"
            ;;
        regression)
            log_info "Running regression detection benchmarks"
            run_quick_benchmarks "$json_output" true
            if [[ "$compare_results_flag" == "true" ]]; then
                compare_results "block-write"
                compare_results "block-read"
                compare_results "single-query"
            fi
            ;;
        *)
            log_error "Unknown benchmark type: $benchmark_type"
            exit 1
            ;;
    esac
    
    log_info "Benchmark execution completed"
}

# Run main function
main "$@"