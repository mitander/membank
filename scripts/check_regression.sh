#!/usr/bin/env bash
set -euo pipefail

# Performance regression detection for KausalDB
# Compares current benchmark results against established baseline

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASELINE_FILE="$PROJECT_ROOT/.github/performance-baseline.json"
TEMP_DIR=$(mktemp -d)
CURRENT_RESULTS="$TEMP_DIR/current_results.json"

trap 'rm -rf "$TEMP_DIR"' EXIT

check_dependencies() {
    if ! command -v jq >/dev/null 2>&1; then
        echo "Error: jq required but not installed"
        exit 1
    fi

    if [[ ! -f "$BASELINE_FILE" ]]; then
        echo "Error: Baseline file not found at $BASELINE_FILE"
        echo "Run '$0 baseline' to create initial baseline"
        exit 1
    fi
}

build_benchmark() {
    echo "Building benchmark executable..."
    cd "$PROJECT_ROOT"
    ./zig/zig build benchmark >/dev/null 2>&1 || {
        echo "Error: Benchmark build failed"
        exit 1
    }
}

run_benchmarks() {
    echo "Running benchmarks..."
    cd "$PROJECT_ROOT"

    local storage_results="$TEMP_DIR/storage.json"
    local query_results="$TEMP_DIR/query.json"

    ./zig-out/bin/benchmark storage --json > "$storage_results" 2>/dev/null || {
        echo "Error: Storage benchmark failed"
        exit 1
    }

    ./zig-out/bin/benchmark query --json > "$query_results" 2>/dev/null || {
        echo "Error: Query benchmark failed"
        exit 1
    }

    jq -s 'add' "$storage_results" "$query_results" > "$CURRENT_RESULTS"
}

get_baseline_value() {
    local operation="$1"
    local field="$2"

    # Handle operation name variations
    local baseline_op="$operation"
    [[ "$operation" == "Batch Query" ]] && baseline_op="Batch Query (10 blocks)"

    jq -r --arg op "$baseline_op" --arg field "$field" \
        '.results[]? | select(.operation_name == $op) | .[$field] // empty' \
        "$BASELINE_FILE" 2>/dev/null || echo ""
}

get_current_value() {
    jq -r --arg op "$1" --arg field "$2" \
        '.[]? | select(.operation == $op) | .[$field] // empty' \
        "$CURRENT_RESULTS" 2>/dev/null || echo ""
}

calculate_change() {
    local baseline="$1"
    local current="$2"

    [[ "$baseline" == "0" || -z "$baseline" ]] && { echo "N/A"; return; }

    echo "$baseline $current" | awk '{
        if ($1 == 0) print "N/A"
        else printf "%.1f", (($2 - $1) / $1) * 100
    }'
}

check_regression() {
    local operation="$1"
    local metric="$2"
    local baseline="$3"
    local current="$4"
    local threshold="$5"
    local direction="$6"  # "increase" or "decrease"

    [[ -z "$baseline" || -z "$current" ]] && return 0

    local change=$(calculate_change "$baseline" "$current")
    [[ "$change" == "N/A" ]] && return 0

    local regression=0
    if [[ "$direction" == "increase" ]]; then
        regression=$(echo "$change $threshold" | awk '{print ($1 > $2) ? 1 : 0}')
    else
        regression=$(echo "$change $threshold" | awk '{print ($1 < -$2) ? 1 : 0}')
    fi

    if [[ "$regression" == "1" ]]; then
        echo "REGRESSION: $operation $metric ${change}% ($baseline -> $current)"
        return 1
    else
        echo "OK: $operation $metric ${change}%"
        return 0
    fi
}

analyze_regressions() {
    echo "Analyzing regressions..."

    local regressions=0
    local operations=("Block Write" "Block Read" "Block Update" "Block Delete" "Single Query" "Batch Query" "WAL Flush")

    for operation in "${operations[@]}"; do
        local baseline_latency=$(get_baseline_value "$operation" "mean_ns")
        local baseline_throughput=$(get_baseline_value "$operation" "throughput_ops_per_sec")

        local current_latency=$(get_current_value "$operation" "mean_ns")
        local current_throughput=$(get_current_value "$operation" "throughput_ops_per_sec")

        if ! check_regression "$operation" "latency" "$baseline_latency" "$current_latency" "15.0" "increase"; then
            ((regressions++))
        fi

        if ! check_regression "$operation" "throughput" "$baseline_throughput" "$current_throughput" "10.0" "decrease"; then
            ((regressions++))
        fi
    done

    return $regressions
}

print_summary() {
    local regressions="$1"

    echo ""
    if [[ "$regressions" -eq 0 ]]; then
        echo "PASS: No regressions detected"
    else
        echo "FAIL: $regressions regression(s) detected"
        echo "Thresholds: latency +15%, throughput -10%"
    fi
}

main() {
    echo "KausalDB regression check"
    check_dependencies
    build_benchmark
    run_benchmarks

    local regressions=0
    if ! analyze_regressions; then
        regressions=$?
    fi

    print_summary $regressions
    exit $regressions
}

case "${1:-check}" in
    "check")
        main
        ;;
    "baseline")
        echo "Updating baseline..."
        build_benchmark
        run_benchmarks
        cp "$CURRENT_RESULTS" "$BASELINE_FILE"
        echo "Baseline updated"
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [check|baseline|help]"
        echo ""
        echo "  check     Check for regressions (default)"
        echo "  baseline  Update performance baseline"
        echo "  help      Show help"
        ;;
    *)
        echo "Error: Unknown command '$1'"
        exit 1
        ;;
esac
