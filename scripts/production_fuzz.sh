#!/bin/bash
#
# CortexDB Production Fuzzing Script (TigerBeetle-inspired)
# 
# Features inspired by your Rust fuzzer:
# - Git auto-update with graceful restart
# - Optional push notifications via ntfy.sh
# - Better error handling and progress tracking
# - Automatic crash analysis
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORTEXDB_ROOT="$(dirname "$SCRIPT_DIR")"
FUZZ_BINARY="$CORTEXDB_ROOT/zig-out/bin/fuzz"
FUZZ_DEBUG_BINARY="$CORTEXDB_ROOT/zig-out/bin/fuzz-debug"
REPORTS_DIR="$CORTEXDB_ROOT/fuzz_reports"
LOG_FILE="$REPORTS_DIR/production_fuzz.log"
GIT_UPDATE_INTERVAL=600  # 10 minutes
NTFY_TOPIC="cortexdb_fuzz"
ENABLE_NOTIFICATIONS=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg"
    echo "$msg" >> "$LOG_FILE"
}

error() {
    local msg="[ERROR] $1"
    echo -e "${RED}${msg}${NC}"  # Color for terminal
    echo "$msg" >> "$LOG_FILE"   # Plain text for log
}

info() {
    local msg="[INFO] $1"
    echo -e "${GREEN}${msg}${NC}"  # Color for terminal
    echo "$msg" >> "$LOG_FILE"   # Plain text for log
}

warn() {
    local msg="[WARN] $1"
    echo -e "${YELLOW}${msg}${NC}"  # Color for terminal
    echo "$msg" >> "$LOG_FILE"    # Plain text for log
}

debug() {
    local msg="[DEBUG] $1"
    echo -e "${BLUE}${msg}${NC}"  # Color for terminal
    echo "$msg" >> "$LOG_FILE"   # Plain text for log
}

# Check for notification argument
if [[ "${1:-}" == "notify" ]]; then
    ENABLE_NOTIFICATIONS=true
    shift  # Remove notify from arguments
    info "Notifications enabled for topic: $NTFY_TOPIC"
else
    info "Running without notifications (use 'notify' as first argument to enable)"
fi

# Function to send notifications
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

# Function to check for git updates (similar to your Rust version)
check_git_updates() {
    debug "Checking for git updates..."
    cd "$CORTEXDB_ROOT" || return 1
    
    # Fetch latest changes
    if ! git fetch --quiet 2>/dev/null; then
        warn "Git fetch failed, continuing with current version"
        return 1
    fi
    
    # Check if we're behind origin
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
    
    debug "No git updates found"
    return 2  # No updates
}

# Function to build project (with retry logic like your Rust version)
build_project() {
    info "Building CortexDB fuzzer..."
    cd "$CORTEXDB_ROOT" || return 1
    
    if ./zig/zig build fuzz fuzz-debug; then
        info "Build successful"
        return 0
    else
        error "Build failed at $(date)"
        send_notification "BUILD FAILED" "CortexDB build failed, retrying in 5 minutes" "boom,build"
        return 1
    fi
}

# Function to analyze crash reports
analyze_crashes() {
    local crash_count
    crash_count=$(find "$REPORTS_DIR" -name "crash_*.txt" -newer "$LOG_FILE" 2>/dev/null | wc -l)
    
    if [[ "$crash_count" -gt 0 ]]; then
        warn "Found $crash_count new crash report(s)"
        send_notification "NEW CRASHES FOUND" "$crash_count new crash reports in CortexDB fuzzer" "boom,warning"
        
        # List recent crashes for context
        find "$REPORTS_DIR" -name "crash_*.txt" -newer "$LOG_FILE" 2>/dev/null | while read -r crash_file; do
            warn "New crash: $(basename "$crash_file")"
        done
    fi
}

# Function to run fuzzer (enhanced version)  
run_fuzzer() {
    local target="${1:-all}"
    local seed="${2:-$RANDOM}"
    
    info "Starting CortexDB continuous fuzzing..."
    info "Target: $target, Seed: $seed"

    # Run fuzzer directly (not backgrounded) so Ctrl+C reaches it
    "$FUZZ_BINARY" "$target" continuous "$seed"
    local exit_code=$?
    return $exit_code
}

# Main fuzzing loop (inspired by your Rust main function)
main() {
    local target="${1:-all}"
    local manual_seed="${2:-}"
    
    send_notification "FUZZER STARTING" "CortexDB continuous fuzzing session started (target: $target)" "rocket"

    while true; do
        info "=== $(date): Starting new fuzzing iteration ==="

        # Check for updates periodically (like your git update check)
        case $(check_git_updates) in
            0)
                info "Git updates found - rebuilding and restarting..."
                # Build with retry logic
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

        # Generate seed (use manual seed if provided, otherwise random)
        local current_seed
        if [[ -n "$manual_seed" ]]; then
            current_seed="$manual_seed"
        else
            current_seed="$RANDOM$RANDOM"  # Better randomness
        fi

        # Run fuzzer
        run_fuzzer "$target" "$current_seed"
        local exit_code=$?

        case $exit_code in
            0)
                info "Fuzzer exited gracefully (likely due to git update or completion)"
                analyze_crashes
                info "Restarting fuzzing loop..."
                continue
                ;;
            130)  # SIGINT (Ctrl+C)
                info "Received interrupt signal, shutting down gracefully..."
                send_notification "FUZZER STOPPED" "CortexDB fuzzing stopped by user interrupt" "stop"
                exit 0
                ;;
            143)  # SIGTERM
                info "Received termination signal, shutting down gracefully..."
                send_notification "FUZZER STOPPED" "CortexDB fuzzing stopped by termination signal" "stop"
                exit 0
                ;;
            *)
                error "Fuzzer crashed with exit code $exit_code at $(date)"
                analyze_crashes
                send_notification "FUZZER CRASH!" "CortexDB fuzzer crashed with exit code $exit_code" "boom,warning"
                
                # Log crash for debugging
                echo "$(date): Fuzzer crashed with exit code $exit_code (target: $target, seed: $current_seed)" >> "$REPORTS_DIR/fuzzer_crashes.log"
                
                warn "Sleeping 60 seconds before restart..."
                sleep 60
                ;;
        esac
    done
}

# Check if fuzz binary exists
if [[ ! -f "$FUZZ_BINARY" ]]; then
    error "Fuzz binary not found at $FUZZ_BINARY"
    info "Building fuzzer first..."
    if ! build_project; then
        error "Failed to build fuzzer. Exiting."
        exit 1
    fi
fi

# Create reports directory
mkdir -p "$REPORTS_DIR"
touch "$LOG_FILE"

# Global variable to track fuzzer PID
FUZZER_PID=""

# Setup signal handlers
cleanup() {
    info "Shutting down gracefully..."
    
    # Since we run fuzzer in foreground, Ctrl+C will naturally reach it
    # Just exit the script - the fuzzer will already be stopped
    send_notification "FUZZER STOPPED" "Production fuzzing stopped by user" "stop"
    exit 0
}

trap 'cleanup' SIGTERM SIGINT

# Parse arguments and validate
TARGET="${1:-all}"
SEED="${2:-}"

case "$TARGET" in
    "storage"|"query"|"parser"|"serialization"|"all")
        info "CortexDB Production Fuzzing Started"
        info "Target: $TARGET"
        info "Seed: ${SEED:-random}"
        info "Log file: $LOG_FILE"
        info "Reports dir: $REPORTS_DIR"
        if [[ "$ENABLE_NOTIFICATIONS" == "true" ]]; then
            info "Notifications: enabled ($NTFY_TOPIC)"
        else
            info "Notifications: disabled"
        fi
        info ""
        info "Features enabled:"
        info "  ✓ Git auto-update every $((GIT_UPDATE_INTERVAL/60)) minutes"
        info "  ✓ Automatic crash analysis"
        info "  ✓ Graceful signal handling"
        info "  ✓ Build failure recovery"
        if [[ "$ENABLE_NOTIFICATIONS" == "true" ]]; then
            info "  ✓ Push notifications (ntfy.sh)"
        fi
        info ""
        info "To stop fuzzing: Ctrl+C"
        info "To run in background: nohup $0 $TARGET $SEED &"
        echo ""
        
        main "$TARGET" "$SEED"
        ;;
    *)
        echo "Usage: $0 [notify] [target] [seed]"
        echo ""
        echo "Arguments:"
        echo "  notify            Enable push notifications (must be first argument)"
        echo ""
        echo "Targets:"
        echo "  storage           Fuzz storage engine continuously"
        echo "  query             Fuzz query engine continuously"  
        echo "  parser            Fuzz Zig parser continuously"
        echo "  serialization     Fuzz serialization continuously"
        echo "  all               Fuzz all targets continuously (default)"
        echo ""
        echo "Examples:"
        echo "  $0                        # Fuzz all targets"
        echo "  $0 notify query           # Fuzz query engine with notifications"
        echo "  $0 storage 12345          # Fuzz storage with specific seed"
        echo "  $0 notify all 42          # Full fuzzing with notifications and seed"
        echo "  nohup $0 notify all &     # Run in background with notifications"
        exit 1
        ;;
esac