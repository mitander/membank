#!/bin/bash
set -euo pipefail

# KausalDB Performance CI Script
# Detects performance regressions by comparing current results to baseline

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASELINE_FILE="$REPO_ROOT/.github/performance-baseline.json"
CURRENT_FILE="/tmp/current-performance.json"
REGRESSION_THRESHOLD="1.15"  # 15% slowdown = regression

cd "$REPO_ROOT"

echo "Running KausalDB Performance CI"
echo "================================"

# Build benchmark binary
echo "Building benchmark binary..."
./zig/zig build benchmark > /dev/null 2>&1

# Run benchmarks and capture JSON output
echo "Running performance benchmarks..."
./zig-out/bin/benchmark all --json 2>&1 | sed '/^info(/d' > "$CURRENT_FILE"

if [[ ! -s "$CURRENT_FILE" ]]; then
    echo "Error: Benchmark execution failed or produced no output"
    exit 1
fi

echo "Benchmarks completed successfully"

# Check if baseline exists
if [[ ! -f "$BASELINE_FILE" ]]; then
    echo "No baseline found, creating initial baseline..."
    cp "$CURRENT_FILE" "$BASELINE_FILE"
    echo "Baseline created at $BASELINE_FILE"
    echo "All future runs will be compared against this baseline"
    exit 0
fi

echo "Comparing current results to baseline..."

# Performance regression detection using jq and shell arithmetic
REGRESSION_COUNT=0
IMPROVEMENT_COUNT=0

echo "Checking for performance regressions..."
for OPERATION in $(jq -r '.results[].operation_name' "$CURRENT_FILE"); do
    CURRENT_MEAN=$(jq -r --arg op "$OPERATION" '.results[] | select(.operation_name == $op) | .mean_ns' "$CURRENT_FILE" || echo "0")
    BASELINE_MEAN=$(jq -r --arg op "$OPERATION" '.results[] | select(.operation_name == $op) | .mean_ns' "$BASELINE_FILE" 2>/dev/null || echo "0")

    # Ensure values are numeric and non-empty
    if [[ -z "$CURRENT_MEAN" || "$CURRENT_MEAN" == "null" ]]; then
        CURRENT_MEAN="0"
    fi
    if [[ -z "$BASELINE_MEAN" || "$BASELINE_MEAN" == "null" ]]; then
        BASELINE_MEAN="0"
    fi

    if [[ "$BASELINE_MEAN" != "0" && "$CURRENT_MEAN" != "0" ]]; then
        # Calculate regression ratio using awk for floating point math
        RATIO=$(awk "BEGIN {printf \"%.2f\", $CURRENT_MEAN / $BASELINE_MEAN}")
        IS_REGRESSION=$(awk "BEGIN {print ($RATIO > $REGRESSION_THRESHOLD) ? 1 : 0}")
        IS_IMPROVEMENT=$(awk "BEGIN {print ($RATIO < 0.95) ? 1 : 0}")

        if [[ "$IS_REGRESSION" == "1" ]]; then
            PERCENT_SLOWER=$(awk "BEGIN {printf \"%.1f\", ($RATIO - 1) * 100}")
            echo "  REGRESSION: $OPERATION: ${PERCENT_SLOWER}% slower"
            echo "    ${BASELINE_MEAN}ns -> ${CURRENT_MEAN}ns"
            REGRESSION_COUNT=$((REGRESSION_COUNT + 1))
        elif [[ "$IS_IMPROVEMENT" == "1" ]]; then
            PERCENT_FASTER=$(awk "BEGIN {printf \"%.1f\", (1 - $RATIO) * 100}")
            echo "  IMPROVEMENT: $OPERATION: ${PERCENT_FASTER}% faster"
            echo "    ${BASELINE_MEAN}ns -> ${CURRENT_MEAN}ns"
            IMPROVEMENT_COUNT=$((IMPROVEMENT_COUNT + 1))
        fi
    fi
done

if [[ "$REGRESSION_COUNT" -gt 0 ]]; then
    echo "Performance regressions detected: $REGRESSION_COUNT operations"
    echo "Regression threshold: $(awk "BEGIN {printf \"%.0f\", ($REGRESSION_THRESHOLD - 1) * 100}")% slowdown"
    echo "Consider optimizing the affected operations or updating baseline if intentional"
    exit 1
else
    echo "No performance regressions detected"
    if [[ "$IMPROVEMENT_COUNT" -gt 0 ]]; then
        echo "Found $IMPROVEMENT_COUNT performance improvements"
    fi
    echo "All operations within acceptable performance bounds"
fi

echo "Performance CI passed!"

# Optional: Update baseline on main branch if significantly improved
if [[ "${GITHUB_REF:-}" == "refs/heads/main" || "${GITHUB_REF:-}" == "refs/heads/master" ]]; then
    echo "Main branch: Considering baseline update..."
    # Could add logic here to update baseline if improvements are significant
fi

# Cleanup
rm -f "$CURRENT_FILE"
