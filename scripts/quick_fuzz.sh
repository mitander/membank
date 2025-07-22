#!/bin/bash
#
# Quick CortexDB Fuzzing Script
# 
# Simple, no-frills fuzzing for development and testing
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORTEXDB_ROOT="$(dirname "$SCRIPT_DIR")"
FUZZ_BINARY="$CORTEXDB_ROOT/zig-out/bin/fuzz"

# Parse arguments
TARGET="${1:-all}"
ITERATIONS="${2:-10000}"
SEED="${3:-$RANDOM}"

# Validate target
case "$TARGET" in
    "storage"|"query"|"parser"|"serialization"|"all")
        ;;
    *)
        echo "Usage: $0 [target] [iterations] [seed]"
        echo ""
        echo "Targets: storage, query, parser, serialization, all"
        echo "Examples:"
        echo "  $0 storage 50000      # 50k storage iterations"
        echo "  $0 all 100000 42      # 100k all targets with seed 42"
        exit 1
        ;;
esac

# Check if binary exists
if [[ ! -f "$FUZZ_BINARY" ]]; then
    echo "Building fuzzer..."
    cd "$CORTEXDB_ROOT" && ./zig/zig build fuzz
fi

echo "Quick CortexDB Fuzzing"
echo "======================"
echo "Target: $TARGET"
echo "Iterations: $ITERATIONS"
echo "Seed: $SEED"
echo ""

# Setup Ctrl+C handler for immediate termination
cleanup() {
    echo ""
    echo "Stopping fuzzer..."
    exit 130  # Standard Ctrl+C exit code
}

trap 'cleanup' SIGINT

# Run fuzzer
"$FUZZ_BINARY" "$TARGET" "$ITERATIONS" "$SEED"

# Check for crashes
crash_count=$(find "$CORTEXDB_ROOT/fuzz_reports" -name "crash_*.txt" -type f 2>/dev/null | wc -l)
if [[ "$crash_count" -gt 0 ]]; then
    echo ""
    echo "⚠️  Found $crash_count crash report(s) in fuzz_reports/"
    echo "Latest crashes:"
    find "$CORTEXDB_ROOT/fuzz_reports" -name "crash_*.txt" -type f -exec ls -t {} + 2>/dev/null | head -3 | while read -r f; do
        echo "  - $(basename "$f")"
    done
else
    echo ""
    echo "✅ No crashes found - fuzzing completed successfully"
fi