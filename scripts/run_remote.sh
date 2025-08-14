#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values (can be overridden by env vars)
DEFAULT_SERVER="${KAUSAL_SERVER:-mitander@server}"
DEFAULT_REMOTE_PATH="${KAUSAL_PATH:-~/kausaldb}"
DEFAULT_BUILD_TARGET="test-simulation"
DEFAULT_OPTIMIZE="-Doptimize=ReleaseSafe"

# Parse arguments
SERVER="$DEFAULT_SERVER"
BUILD_TARGET="$DEFAULT_BUILD_TARGET"
REMOTE_PATH="$DEFAULT_REMOTE_PATH"
OPTIMIZE="$DEFAULT_OPTIMIZE"
SYNC_ONLY=false
DRY_RUN=false
VERBOSE=false
INSTALL_ZIG=true
RUN_SCRIPT=""

show_help() {
    cat << 'EOF'
Usage: run_remote.sh [BUILD_TARGET] [OPTIONS]

Enhanced remote testing for KausalDB development.

BUILD TARGETS:
    test-all           Run full test suite
    test-simulation    Run simulation tests (default)
    test               Run unit tests
    run                Run main executable
    fmt                Format code
    tidy               Code quality checks
    benchmark          Run benchmarks
    fuzz               Run fuzzing tests
    <custom>           Any zig build target

OPTIONS:
    --help, -h                 Show this help message
    --server HOST              Remote server (default: mitander@server, env: KAUSAL_SERVER)
    --path PATH                Remote path (default: ~/kausaldb, env: KAUSAL_PATH)
    --sync-only, -s            Only sync files, don't run command
    --dry-run, -n              Show what would be synced without doing it
    --verbose, -v              Verbose output
    --debug                    Use Debug build instead of ReleaseSafe
    --no-zig-install           Skip zig installation check
    --run-script "NAME [ARGS]" Run a script from scripts/ directory remotely with optional arguments

EXAMPLES:
    ./scripts/run_remote.sh test-all                               # Full test suite
    ./scripts/run_remote.sh test-simulation --debug                # Debug build
    ./scripts/run_remote.sh fmt --server user@host                 # Format on custom server
    ./scripts/run_remote.sh --sync-only                            # Just sync files
    ./scripts/run_remote.sh --dry-run                              # Preview sync
    ./scripts/run_remote.sh --run-script install_zig.sh            # Run script remotely
    ./scripts/run_remote.sh --run-script "fuzz.sh --duration 300"  # Run script with arguments

ENVIRONMENT:
    KAUSAL_SERVER     Default remote server
    KAUSAL_PATH       Default remote path
EOF
}

# Parse command line arguments
# First argument is build target if it doesn't start with --
if [[ $# -gt 0 && ! "$1" =~ ^-- ]]; then
    BUILD_TARGET="$1"
    shift
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --server)
            SERVER="$2"
            shift 2
            ;;
        --path)
            REMOTE_PATH="$2"
            shift 2
            ;;
        --sync-only|-s)
            SYNC_ONLY=true
            shift
            ;;
        --dry-run|-n)
            DRY_RUN=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --debug)
            OPTIMIZE="-Doptimize=Debug"
            shift
            ;;
        --no-zig-install)
            INSTALL_ZIG=false
            shift
            ;;
        --run-script)
            RUN_SCRIPT="$2"
            shift 2
            ;;
        --*)
            echo -e "${RED}[!] Error: Unknown option $1${NC}" >&2
            echo "Use --help for usage information"
            exit 1
            ;;
        *)
            echo -e "${RED}[!] Error: Unexpected argument $1${NC}" >&2
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build the command
if [[ -n "$RUN_SCRIPT" ]]; then
    # Parse script name and arguments
    read -r script_name script_args <<< "$RUN_SCRIPT"
    if [[ -n "$script_args" ]]; then
        TEST_CMD="bash scripts/$script_name $script_args"
    else
        TEST_CMD="bash scripts/$script_name"
    fi
else
    TEST_CMD="./zig/zig build $BUILD_TARGET $OPTIMIZE"
fi

# Functions
log_info() {
    echo -e "${BLUE}[i]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[+]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

log_error() {
    echo -e "${RED}[-]${NC} $1"
}

# Check and install zig if needed
ensure_zig_installed() {
    if [[ "$INSTALL_ZIG" == "false" ]]; then
        log_info "Skipping zig installation check"
        return 0
    fi

    log_info "Checking zig installation on $SERVER..."
    if ssh "$SERVER" "cd $REMOTE_PATH && test -f zig/zig && ./zig/zig version >/dev/null 2>&1"; then
        log_success "Zig is already installed"
        return 0
    else
        log_warning "Zig not found or not working, installing..."
        if ssh "$SERVER" "cd $REMOTE_PATH && bash scripts/install_zig.sh"; then
            log_success "Zig installation completed"
            return 0
        else
            log_error "Zig installation failed"
            return 1
        fi
    fi
}

# Check if server is reachable
check_connection() {
    log_info "Testing connection to $SERVER..."
    if ssh -o ConnectTimeout=5 -o BatchMode=yes "$SERVER" exit 2>/dev/null; then
        log_success "Connection to $SERVER successful"
        return 0
    else
        log_error "Cannot connect to $SERVER"
        log_info "Make sure SSH keys are set up and server is reachable"
        return 1
    fi
}

# Sync files
sync_files() {
    local rsync_opts=(-avz --delete --progress)
    local excludes=(
        '.git'
        '*cache'
        'zig-cache'
        '.zig-cache'
        'zig-out'
        'zig'
        '*.tmp'
        '.DS_Store'
        'node_modules'
        'target'
        '.vscode/settings.json'
    )

    # Add excludes
    for exclude in "${excludes[@]}"; do
        rsync_opts+=(--exclude="$exclude")
    done

    # Add dry-run if requested
    if [[ "$DRY_RUN" == "true" ]]; then
        rsync_opts+=(--dry-run)
        log_info "DRY RUN - showing what would be synced:"
    else
        log_info "Syncing local changes to $SERVER:$REMOTE_PATH..."
    fi

    # Add verbose if requested
    if [[ "$VERBOSE" == "true" ]]; then
        rsync_opts+=(--verbose)
    fi

    # Sync main project files (including scripts directory)
    if rsync "${rsync_opts[@]}" . "$SERVER:$REMOTE_PATH/"; then
        if [[ "$DRY_RUN" == "true" ]]; then
            log_success "Dry run completed - no files were actually transferred"
        else
            log_success "File sync completed (including scripts directory)"
        fi
    else
        log_error "File sync failed"
        return 1
    fi

    return 0
}

# Run remote command
run_remote_command() {
    log_info "Running command on $SERVER: $TEST_CMD"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    if ssh "$SERVER" "cd $REMOTE_PATH && $TEST_CMD"; then
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        log_success "Remote command completed successfully"
        return 0
    else
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        log_error "Remote command failed"
        return 1
    fi
}

# Main execution
main() {
    echo -e "${BLUE}[*] KausalDB Remote Test Runner${NC}"
    echo -e "${BLUE}────────────────────────────────────────${NC}"

    # Show configuration
    log_info "Server: $SERVER"
    log_info "Remote path: $REMOTE_PATH"
    if [[ "$SYNC_ONLY" == "false" ]]; then
        log_info "Command: $TEST_CMD"
    fi
    echo

    # Check connection
    if ! check_connection; then
        exit 1
    fi

    # Install zig if needed (only for zig build commands, not scripts)
    if [[ "$SYNC_ONLY" == "false" ]] && [[ "$DRY_RUN" == "false" ]] && [[ -z "$RUN_SCRIPT" ]]; then
        if ! ensure_zig_installed; then
            exit 1
        fi
        echo
    fi

    # Sync files
    if ! sync_files; then
        exit 1
    fi

    # Run command unless sync-only
    if [[ "$SYNC_ONLY" == "false" ]] && [[ "$DRY_RUN" == "false" ]]; then
        echo
        if ! run_remote_command; then
            exit 1
        fi
    elif [[ "$SYNC_ONLY" == "true" ]]; then
        log_info "Sync-only mode: skipping remote command execution"
    fi

    echo
    log_success "All operations completed successfully!"
}

# Run main function
main
