#!/bin/bash
#
# CortexDB Unified Fuzzing Script
#
# Consolidates quick, continuous, and production fuzzing into a single script
# with profile-based configuration for different use cases.
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORTEXDB_ROOT="$(dirname "$SCRIPT_DIR")"
FUZZ_BINARY="$CORTEXDB_ROOT/zig-out/bin/fuzz"
FUZZ_DEBUG_BINARY="$CORTEXDB_ROOT/zig-out/bin/fuzz-debug"
REPORTS_DIR="$CORTEXDB_ROOT/fuzz_reports"
LOG_FILE="$REPORTS_DIR/fuzz.log"
NTFY_TOPIC="cortexdb_fuzz"
ENABLE_NOTIFICATIONS=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Profile configurations (using functions for compatibility)
get_profile_iterations() {
    case "$1" in
        "quick") echo "10000" ;;
        "ci") echo "50000" ;;
        "deep") echo "500000" ;;
        "continuous") echo "continuous" ;;
        "production") echo "continuous" ;;
        *) echo "" ;;
    esac
}

get_profile_description() {
    case "$1" in
        "quick") echo "Quick 5-minute fuzz for local development" ;;
        "ci") echo "Balanced 20-minute fuzz for CI/PR validation" ;;
        "deep") echo "Intensive multi-hour fuzz for nightly testing" ;;
        "continuous") echo "Restart loop with git updates (simple)" ;;
        "production") echo "Full-featured continuous fuzzing with notifications" ;;
        *) echo "" ;;
    esac
}

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg"
    if [[ -f "$LOG_FILE" ]]; then
        echo "$msg" >> "$LOG_FILE"
    fi
}

error() {
    local msg="[ERROR] $1"
    echo -e "${RED}${msg}${NC}"
    if [[ -f "$LOG_FILE" ]]; then
        echo "$msg" >> "$LOG_FILE"
    fi
}

info() {
    local msg="[INFO] $1"
    echo -e "${GREEN}${msg}${NC}"
    if [[ -f "$LOG_FILE" ]]; then
        echo "$msg" >> "$LOG_FILE"
    fi
}

warn() {
    local msg="[WARN] $1"
    echo -e "${YELLOW}${msg}${NC}"
    if [[ -f "$LOG_FILE" ]]; then
        echo "$msg" >> "$LOG_FILE"
    fi
}

debug() {
    local msg="[DEBUG] $1"
    echo -e "${BLUE}${msg}${NC}"
    if [[ -f "$LOG_FILE" ]]; then
        echo "$msg" >> "$LOG_FILE"
    fi
}

# Function to send notifications (production profile only)
send_notification() {
    local title="$1"
    local message="$2"
    local tags="${3:-warning}"

    if [[ "$ENABLE_NOTIFICATIONS" == "true" ]]; then
        debug "Sending notification: $title"
        curl -s -X POST "https://ntfy.sh/${NTFY_TOPIC}" \
            -H "Title: $title" \
            -H "Tags: $tags" \
            -d "$message" || warn "Failed to send notification"
    fi
}

# Function to check for git updates
check_git_updates() {
    debug "Checking for git updates..."
    cd "$CORTEXDB_ROOT" || return 1

    if ! git fetch --quiet 2>/dev/null; then
        warn "Git fetch failed, continuing with current version"
        return 1
    fi

    local behind_count
    behind_count=$(git rev-list --count HEAD..origin/$(git rev-parse --abbrev-ref HEAD) 2>/dev/null || echo "0")

    if [[ "$behind_count" -gt 0 ]]; then
        info "Found $behind_count new commit(s). Updating..."
        if git pull --rebase --quiet; then
            info "Git update successful"
            return 0  # Updates found and applied
        else
            error "Git pull failed"
            return 1
        fi
    fi

    return 2  # No updates
}

# Function to build project (for continuous modes that need persistent binary)
build_project() {
    info "Building CortexDB fuzzer..."
    cd "$CORTEXDB_ROOT" || return 1

    if ./zig/zig build fuzz fuzz-debug -Doptimize=ReleaseSafe; then
        info "Build successful"
        return 0
    else
        error "Build failed"
        return 1
    fi
}

# Function to run fuzzer with always-fresh code
run_fuzzer() {
    local verbose_flag="$1"
    local target="$2"
    local iterations="$3"
    local seed="$4"

    cd "$CORTEXDB_ROOT" || return 1

    # Build and run with fresh code - faster than persistent binary for development
    if [[ "$verbose_flag" == "--verbose" ]]; then
        ./zig/zig build fuzz -Doptimize=ReleaseSafe && ./zig-out/bin/fuzz --verbose "$target" "$iterations" "$seed"
    else
        ./zig/zig build fuzz -Doptimize=ReleaseSafe && ./zig-out/bin/fuzz "$target" "$iterations" "$seed"
    fi
}

# Function to analyze crash reports
analyze_crashes() {
    local crash_count
    crash_count=$(find "$REPORTS_DIR" -name "crash_*.txt" -type f 2>/dev/null | wc -l)

    if [[ "$crash_count" -gt 0 ]]; then
        warn "Found $crash_count crash report(s)"
        send_notification "NEW CRASHES FOUND" "$crash_count crash reports in CortexDB fuzzer" "boom,warning"

        echo "Latest crashes:"
        find "$REPORTS_DIR" -name "crash_*.txt" -type f -exec ls -t {} + 2>/dev/null | head -3 | while read -r f; do
            echo "  - $(basename "$f")"
        done
    else
        info "No crashes found - fuzzing completed successfully"
    fi
}

# Quick fuzzing function (single run)
run_quick_fuzz() {
    local target="$1"
    local iterations="$2"
    local seed="${3:-$RANDOM}"

    info "Quick CortexDB Fuzzing"
    info "======================"
    info "Target: $target"
    info "Iterations: $iterations"
    info "Seed: $seed"
    echo ""

    # Setup Ctrl+C handler for immediate termination
    trap 'echo ""; info "Stopping fuzzer..."; exit 130' SIGINT

    # Run fuzzer with fresh code
    run_fuzzer "$VERBOSE_FLAG" "$target" "$iterations" "$seed"

    # Check for crashes
    analyze_crashes
}

# Continuous fuzzing function (restart loop)
run_continuous_fuzz() {
    local target="$1"
    local profile="$2"
    local seed="${3:-$RANDOM}"

    # Enable notifications for production profile
    if [[ "$profile" == "production" ]]; then
        ENABLE_NOTIFICATIONS=true
        send_notification "FUZZER STARTING" "CortexDB continuous fuzzing started (target: $target)" "rocket"
    fi

    while true; do
        info "=== $(date): Starting new fuzzing iteration ==="

        # Check for updates (continuous and production profiles)
        if [[ "$profile" == "continuous" || "$profile" == "production" ]]; then
            case $(check_git_updates) in
                0)
                    info "Git updates found - rebuilding and restarting..."
                    while ! build_project; do
                        warn "Build failed, sleeping 5 minutes before retry..."
                        sleep 300
                    done
                    send_notification "FUZZER RESTARTED" "Updated to latest code and restarted" "arrows_counterclockwise"
                    ;;
                1)
                    warn "Git update failed, continuing with current version"
                    ;;
                2)
                    debug "No git updates, continuing"
                    ;;
            esac
        fi

        # Check if we need to rebuild (any source files newer than binary)
        local rebuild_needed=false
        if [[ ! -f "$FUZZ_BINARY" ]]; then
            rebuild_needed=true
            debug "Fuzz binary not found"
        else
            # Check all relevant source directories and build files
            for dir in "src" "tests" "build.zig"; do
                if [[ -d "$CORTEXDB_ROOT/$dir" ]] || [[ -f "$CORTEXDB_ROOT/$dir" ]]; then
                    if find "$CORTEXDB_ROOT/$dir" -newer "$FUZZ_BINARY" -print -quit 2>/dev/null | grep -q .; then
                        rebuild_needed=true
                        debug "Changes detected in $dir"
                        break
                    fi
                fi
            done
        fi

        if [[ "$rebuild_needed" == "true" ]]; then
            info "Source files changed, rebuilding fuzzer binary..."
            if ! build_project; then
                error "Build failed, sleeping 1 minute before retry..."
                sleep 60
                continue
            fi
        fi

        # Run fuzzer
        info "Starting fuzzer (target: $target, seed: $seed)"
        "$FUZZ_BINARY" "$target" continuous "$seed"
        local exit_code=$?

        case $exit_code in
            0)
                info "Fuzzer exited gracefully"
                analyze_crashes
                info "Restarting fuzzing loop..."
                ;;
            130)  # SIGINT (Ctrl+C)
                info "Received interrupt signal, shutting down..."
                send_notification "FUZZER STOPPED" "CortexDB fuzzing stopped by user" "stop"
                exit 0
                ;;
            143)  # SIGTERM
                info "Received termination signal, shutting down..."
                send_notification "FUZZER STOPPED" "CortexDB fuzzing stopped by signal" "stop"
                exit 0
                ;;
            *)
                error "Fuzzer crashed with exit code $exit_code"
                analyze_crashes
                send_notification "FUZZER CRASH!" "CortexDB fuzzer crashed with exit code $exit_code" "boom"

                echo "$(date): Fuzzer crashed with exit code $exit_code (target: $target, seed: $seed)" >> "$REPORTS_DIR/fuzzer_crashes.log"
                warn "Sleeping 60 seconds before restart..."
                sleep 60
                ;;
        esac

        # Increment seed for variety
        seed=$((seed + 1))
    done
}

# Main function
main() {
    local profile="$1"
    local target="${2:-all}"
    local seed="${3:-}"

    # Validate target
    case "$target" in
        "storage"|"query"|"parser"|"serialization"|"all")
            ;;
        *)
            error "Invalid target: $target"
            echo "Valid targets: storage, query, parser, serialization, all"
            exit 1
            ;;
    esac

    # Only build binary for continuous modes (quick/ci/deep use zig run)
    if [[ "$profile" == "continuous" || "$profile" == "production" ]]; then
        if [[ ! -f "$FUZZ_BINARY" ]]; then
            info "Fuzz binary not found, building..."
            if ! build_project; then
                error "Failed to build fuzzer"
                exit 1
            fi
        fi
    fi

    # Create reports directory
    mkdir -p "$REPORTS_DIR"
    touch "$LOG_FILE"

    # Run based on profile
    case "$profile" in
        "quick"|"ci"|"deep")
            local iterations
            iterations=$(get_profile_iterations "$profile")
            run_quick_fuzz "$target" "$iterations" "$seed"
            ;;
        "continuous"|"production")
            # Setup signal handlers for continuous modes
            trap 'info "Fuzzing stopped by user"; exit 0' SIGINT SIGTERM
            run_continuous_fuzz "$target" "$profile" "$seed"
            ;;
        *)
            error "Invalid profile: $profile"
            exit 1
            ;;
    esac
}

# Show usage
show_usage() {
    echo "Usage: $0 <profile> [target] [seed]"
    echo ""
    echo "Profiles:"
    for profile in "quick" "ci" "deep" "continuous" "production"; do
        local desc
        desc=$(get_profile_description "$profile")
        echo "  $profile$(printf '%*s' $((12 - ${#profile})) '') $desc"
    done
    echo ""
    echo "Targets: storage, query, parser, serialization, all (default: all)"
    echo ""
    echo "Examples:"
    echo "  $0 quick                    # Quick 5-minute fuzz for development"
    echo "  $0 ci storage              # CI validation for storage engine"
    echo "  $0 deep all 12345          # Deep fuzz all targets with seed 12345"
    echo "  $0 continuous              # Simple continuous fuzzing"
    echo "  $0 production storage      # Full-featured production fuzzing"
    echo "  nohup $0 production &      # Run production fuzzing in background"
    echo ""
    echo "Notes:"
    echo "  - Production profile enables push notifications to ntfy.sh"
    echo "  - Continuous/production profiles auto-update from git"
    echo "  - Use Ctrl+C to stop any fuzzing session gracefully"
}

# Parse arguments
if [[ $# -eq 0 ]]; then
    show_usage
    exit 1
fi

# Check for verbose flag first
VERBOSE_FLAG=""
if [[ "$1" == "--verbose" || "$1" == "-v" ]]; then
    VERBOSE_FLAG="--verbose"
    shift
fi

PROFILE="$1"
TARGET="${2:-all}"
SEED="${3:-}"

# Validate profile
if [[ -z "$(get_profile_iterations "$PROFILE")" ]]; then
    error "Invalid profile: $PROFILE"
    echo ""
    show_usage
    exit 1
fi

# Show what we're about to do
info "CortexDB Fuzzing"
info "================"
info "Profile: $PROFILE ($(get_profile_description "$PROFILE"))"
info "Target: $TARGET"
info "Seed: ${SEED:-random}"
info "Reports dir: $REPORTS_DIR"
echo ""

# Run main function
main "$PROFILE" "$TARGET" "$SEED"
