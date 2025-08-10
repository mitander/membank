#!/bin/bash
set -euo pipefail

# KausalDB Performance Measurement Script
# Gathers real performance data to set appropriate CI thresholds

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

# Configuration
ITERATIONS=10
OUTPUT_FILE="/tmp/kausaldb_performance_measurements.json"
LOAD_PROCESSES=4

echo "KausalDB Performance Measurement"
echo "================================"
echo "Iterations: $ITERATIONS"
echo "Output: $OUTPUT_FILE"
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_step() {
    echo -e "${YELLOW}-> $1${NC}"
}

log_success() {
    echo -e "${GREEN}+ $1${NC}"
}

# Ensure benchmark is built
log_step "Building benchmark"
./zig/zig build benchmark > /dev/null 2>&1

# Function to extract latency from test output
extract_latency() {
    local output="$1"
    # Look for "block write operation: XXXXns" pattern
    echo "$output" | grep -o "block write operation: [0-9]*ns" | grep -o "[0-9]*" || echo "0"
}

# Function to run performance test and extract timing
run_performance_test() {
    local scenario="$1"
    local env_vars="$2"

    log_step "Running $scenario scenario ($ITERATIONS iterations)"

    local latencies=()
    local success_count=0

    for ((i=1; i<=ITERATIONS; i++)); do
        echo -n "  Iteration $i/$ITERATIONS... "

        # Run the test with environment variables
        local output
        if output=$(env $env_vars ./zig/zig build performance_streaming_memory_benchmark -Doptimize=ReleaseSafe 2>&1); then
            local latency=$(extract_latency "$output")
            if [[ "$latency" != "0" ]]; then
                latencies+=("$latency")
                success_count=$((success_count + 1))
                echo "${latency}ns"
            else
                echo "FAILED (no latency data)"
            fi
        else
            echo "FAILED (test error)"
        fi
    done

    if [[ ${#latencies[@]} -eq 0 ]]; then
        echo "  No successful measurements for $scenario"
        return 1
    fi

    # Calculate statistics
    local sorted=($(printf '%s\n' "${latencies[@]}" | sort -n))
    local count=${#sorted[@]}
    local sum=0
    local min=${sorted[0]}
    local max=${sorted[$((count-1))]}

    for latency in "${sorted[@]}"; do
        sum=$((sum + latency))
    done

    local mean=$((sum / count))
    local p95_index=$(( (95 * count) / 100 ))
    local p99_index=$(( (99 * count) / 100 ))
    local p95=${sorted[$p95_index]}
    local p99=${sorted[$p99_index]}

    echo "  Results: $success_count/$ITERATIONS successful"
    echo "  Min: ${min}ns, Mean: ${mean}ns, P95: ${p95}ns, P99: ${p99}ns, Max: ${max}ns"

    # Store results in JSON format
    cat >> "$OUTPUT_FILE" << EOF
    {
      "scenario": "$scenario",
      "iterations": $count,
      "success_rate": $(echo "scale=2; $success_count * 100 / $ITERATIONS" | bc -l),
      "min_ns": $min,
      "mean_ns": $mean,
      "p95_ns": $p95,
      "p99_ns": $p99,
      "max_ns": $max,
      "raw_data": [$(IFS=','; echo "${sorted[*]}")]
    },
EOF

    echo
}

# Function to create CPU load
create_cpu_load() {
    local processes=$1
    local pids=()

    for ((i=0; i<processes; i++)); do
        # Create CPU load using dd and compression
        dd if=/dev/zero bs=1M count=100 2>/dev/null | gzip > /dev/null &
        pids+=($!)
    done

    echo "${pids[@]}"
}

# Function to stop load processes
stop_load() {
    local pids=("$@")
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}

# Initialize output file
cat > "$OUTPUT_FILE" << EOF
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "system": "$(uname -a)",
  "measurements": [
EOF

# Scenario 1: Isolated (best case)
run_performance_test "isolated" ""

# Scenario 2: Parallel tests environment
run_performance_test "parallel_env" "KAUSALDB_PARALLEL_TESTS=true"

# Scenario 3: Under CPU load (simulating busy CI runner)
log_step "Creating CPU load ($LOAD_PROCESSES processes)"
load_pids=($(create_cpu_load $LOAD_PROCESSES))
sleep 2  # Let load stabilize

run_performance_test "cpu_load" ""

log_step "Stopping CPU load"
stop_load "${load_pids[@]}"
sleep 2  # Let system settle

# Scenario 4: Parallel environment under load (worst case)
log_step "Creating CPU load for parallel scenario"
load_pids=($(create_cpu_load $LOAD_PROCESSES))
sleep 2

run_performance_test "parallel_with_load" "KAUSALDB_PARALLEL_TESTS=true"

stop_load "${load_pids[@]}"

# Finalize JSON (remove trailing comma and close)
sed -i '$ s/,$//' "$OUTPUT_FILE"
cat >> "$OUTPUT_FILE" << EOF
  ],
  "recommendations": {
    "target_latency_ns": 50000,
    "local_multiplier": "TBD",
    "parallel_multiplier": "TBD",
    "ci_multiplier": "TBD"
  }
}
EOF

log_success "Measurements complete!"
echo
echo "Results saved to: $OUTPUT_FILE"
echo

# Analyze results and provide recommendations
log_step "Performance Analysis"

echo "Raw measurements:"
cat "$OUTPUT_FILE"

echo
echo "Threshold Recommendations:"
echo "=========================="

# Extract values for analysis
isolated_mean=$(jq -r '.measurements[] | select(.scenario=="isolated") | .mean_ns' "$OUTPUT_FILE" 2>/dev/null || echo "50000")
parallel_mean=$(jq -r '.measurements[] | select(.scenario=="parallel_env") | .mean_ns' "$OUTPUT_FILE" 2>/dev/null || echo "100000")
load_mean=$(jq -r '.measurements[] | select(.scenario=="cpu_load") | .mean_ns' "$OUTPUT_FILE" 2>/dev/null || echo "200000")
worst_mean=$(jq -r '.measurements[] | select(.scenario=="parallel_with_load") | .mean_ns' "$OUTPUT_FILE" 2>/dev/null || echo "500000")

# Calculate multipliers (using bc for floating point if available)
if command -v bc >/dev/null 2>&1; then
    local_mult=$(echo "scale=1; $isolated_mean / 50000 * 1.5" | bc -l)
    parallel_mult=$(echo "scale=1; $parallel_mean / 50000 * 1.5" | bc -l)
    ci_mult=$(echo "scale=1; $worst_mean / 50000 * 2.0" | bc -l)
else
    local_mult=$(( (isolated_mean * 15) / (50000 * 10) ))
    parallel_mult=$(( (parallel_mean * 15) / (50000 * 10) ))
    ci_mult=$(( (worst_mean * 20) / (50000 * 10) ))
fi

echo "Target latency: 50,000ns (50µs)"
echo "Isolated mean:  ${isolated_mean}ns -> Local multiplier: ${local_mult}x"
echo "Parallel mean:  ${parallel_mean}ns -> Parallel multiplier: ${parallel_mult}x"
echo "CPU load mean:  ${load_mean}ns"
echo "Worst case:     ${worst_mean}ns -> CI multiplier: ${ci_mult}x"
echo
echo "Suggested thresholds:"
echo ".local => { .latency = ${local_mult} }     // Development machine"
echo ".parallel => { .latency = ${parallel_mult} }  // Parallel test execution"
echo ".ci => { .latency = ${ci_mult} }           // GitHub runners"
echo
echo "Update these values in src/test/performance_assertions.zig"
