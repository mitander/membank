#!/bin/bash
#
# CortexDB Continuous Fuzzing Script (inspired by rohcstar pattern)
#
# Simple restart loop - the fuzzer binary handles git updates internally
#

set -euo pipefail

# Setup directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORTEXDB_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$CORTEXDB_ROOT" || exit 1
echo "Working from CortexDB root: $CORTEXDB_ROOT"

FUZZ_BINARY="./zig-out/bin/fuzz"
REPORTS_DIR="./fuzz_reports"

# Colors for output  
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Check if fuzz binary exists
if [[ ! -f "$FUZZ_BINARY" ]]; then
    error "Fuzz binary not found at $FUZZ_BINARY"
    info "Please run: ./zig/zig build fuzz"
    exit 1
fi

# Check if debug fuzz binary exists (no variable defined yet)
# if [[ ! -f "$FUZZ_DEBUG_BINARY" ]]; then
#     warn "Debug fuzz binary not found at $FUZZ_DEBUG_BINARY"
#     warn "Crash reproduction will be limited. Consider running: ./zig/zig build fuzz-debug"
# fi

# Create reports directory
mkdir -p "$REPORTS_DIR"

# Function to build project (with retry logic like rohcstar)
build_project() {
    echo "Building project..."
    if ./zig/zig build fuzz; then
        echo "Build successful"
        return 0
    else
        echo "Build failed at $(date)"
        return 1
    fi
}

# Function to run fuzzer (simplified like rohcstar)
run_fuzzer() {
    local target="${1:-all}"
    local seed="${2:-$RANDOM}"
    
    echo "Starting CortexDB continuous fuzz..."
    echo "Target: $target, Seed: $seed"

    # Run fuzzer - it handles git updates internally and exits with 0 when restart needed
    "$FUZZ_BINARY" "$target" continuous "$seed"
    return $?
}

# Main loop (inspired by rohcstar pattern)
main() {
    local target="${1:-all}"
    local seed="${2:-$RANDOM}"

    while true; do
        echo "=== $(date): Starting new iteration ==="

        # Pull latest changes (let git handle any conflicts)
        echo "Pulling latest changes..."
        if ! git pull --rebase; then
            echo "Git pull failed, continuing with current version"
        fi

        # Build project (retry on failure)
        while ! build_project; do
            echo "Sleeping 5 minutes before build retry..."
            sleep 300
        done

        # Run fuzzer
        run_fuzzer "$target" "$seed"
        exit_code=$?

        case $exit_code in
            0)
                echo "Fuzzer exited gracefully (likely due to git update check)"
                echo "Restarting with updated code..."
                continue
                ;;
            130)  # SIGINT (Ctrl+C)
                echo "Received interrupt signal, exiting..."
                exit 0
                ;;
            *)
                echo "Fuzzer crashed with exit code $exit_code at $(date)"
                echo "$(date): Exit code $exit_code" >> "$REPORTS_DIR/fuzzer_crashes.log"
                echo "Sleeping 60 seconds before restart..."
                sleep 60
                ;;
        esac

        # Increment seed for variety
        seed=$((seed + 1))
    done
}

# Setup signal handlers - simple like rohcstar
trap 'info "Fuzzing stopped by user"; exit 0' SIGINT SIGTERM

# Parse command line arguments and run
TARGET="${1:-all}"
SEED="${2:-$RANDOM}"

case "$TARGET" in
    "storage"|"query"|"parser"|"serialization"|"all")
        info "CortexDB Continuous Fuzzing Started (rohcstar pattern)"
        info "Target: $TARGET"
        info "Seed: $SEED"
        info "Reports dir: $REPORTS_DIR"
        info ""
        info "To stop fuzzing, press Ctrl+C"
        info "To run in background: nohup $0 $TARGET $SEED &"
        info ""

        main "$TARGET" "$SEED"
        ;;
    *)
        echo "Usage: $0 [target] [seed]"
        echo ""
        echo "Targets:"
        echo "  storage       Fuzz storage engine continuously"
        echo "  query         Fuzz query engine continuously"
        echo "  parser        Fuzz Zig parser continuously"
        echo "  serialization Fuzz serialization continuously"
        echo "  all           Fuzz all targets continuously (default)"
        echo ""
        echo "Examples:"
        echo "  $0                    # Fuzz all targets"
        echo "  $0 query              # Fuzz query engine only"
        echo "  $0 storage 12345      # Fuzz storage with seed 12345"
        echo "  nohup $0 all &        # Run in background"
        exit 1
        ;;
esac
