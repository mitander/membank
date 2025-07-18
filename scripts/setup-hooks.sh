#!/bin/bash
#
# CortexDB Git Hooks Setup Script
#
# This script installs and manages git hooks for CortexDB development.
# Run this after cloning the repository to enable automatic code quality checks.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HOOKS_DIR="$PROJECT_ROOT/.githooks"
GIT_HOOKS_DIR="$PROJECT_ROOT/.git/hooks"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    local status=$1
    local message=$2
    case $status in
        "OK")
            echo -e "${GREEN}[OK]${NC} $message"
            ;;
        "FAIL")
            echo -e "${RED}[FAIL]${NC} $message"
            ;;
        "INFO")
            echo -e "${BLUE}[INFO]${NC} $message"
            ;;
        "WARN")
            echo -e "${YELLOW}[WARN]${NC} $message"
            ;;
    esac
}

check_requirements() {
    print_status "INFO" "Checking requirements..."

    # Check if we're in a git repository
    if ! git rev-parse --git-dir >/dev/null 2>&1; then
        print_status "FAIL" "Not in a git repository"
        exit 1
    fi

    # Check if zig is available
    if ! command -v zig >/dev/null 2>&1; then
        print_status "WARN" "Zig compiler not found - hooks will fail until Zig is installed"
    else
        print_status "OK" "Zig compiler found"
    fi

    # Check if hooks directory exists
    if [ ! -d "$HOOKS_DIR" ]; then
        print_status "FAIL" "Hooks directory not found: $HOOKS_DIR"
        exit 1
    fi

    print_status "OK" "Requirements check completed"
}

install_hooks() {
    print_status "INFO" "Installing git hooks..."

    # Create git hooks directory if it doesn't exist
    mkdir -p "$GIT_HOOKS_DIR"

    # Install pre-commit hook
    local source_hook="$HOOKS_DIR/pre-commit"
    local target_hook="$GIT_HOOKS_DIR/pre-commit"

    if [ ! -f "$source_hook" ]; then
        print_status "FAIL" "Source hook not found: $source_hook"
        exit 1
    fi

    # Backup existing hook if it exists
    if [ -f "$target_hook" ]; then
        local backup_file="$target_hook.backup.$(date +%s)"
        cp "$target_hook" "$backup_file"
        print_status "INFO" "Backed up existing hook to: $backup_file"
    fi

    # Copy and make executable
    cp "$source_hook" "$target_hook"
    chmod +x "$target_hook"

    print_status "OK" "Pre-commit hook installed"
}

test_hooks() {
    print_status "INFO" "Testing hook installation..."

    local hook_file="$GIT_HOOKS_DIR/pre-commit"

    if [ ! -f "$hook_file" ]; then
        print_status "FAIL" "Hook file not found after installation"
        return 1
    fi

    if [ ! -x "$hook_file" ]; then
        print_status "FAIL" "Hook file is not executable"
        return 1
    fi

    # Test if hook can run (dry run)
    print_status "INFO" "Running hook test..."
    if cd "$PROJECT_ROOT" && SKIP_TESTS=1 "$hook_file" >/dev/null 2>&1; then
        print_status "OK" "Hook test passed"
    else
        print_status "WARN" "Hook test failed - check your development environment"
        print_status "INFO" "Run 'zig build tidy' manually to diagnose issues"
    fi
}

uninstall_hooks() {
    print_status "INFO" "Uninstalling git hooks..."

    local hook_file="$GIT_HOOKS_DIR/pre-commit"

    if [ -f "$hook_file" ]; then
        rm "$hook_file"
        print_status "OK" "Pre-commit hook removed"
    else
        print_status "INFO" "No pre-commit hook found to remove"
    fi
}

show_status() {
    print_status "INFO" "Git hooks status:"
    echo ""

    local hook_file="$GIT_HOOKS_DIR/pre-commit"

    if [ -f "$hook_file" ]; then
        if [ -x "$hook_file" ]; then
            print_status "OK" "Pre-commit hook: installed and executable"
        else
            print_status "WARN" "Pre-commit hook: installed but not executable"
        fi
    else
        print_status "FAIL" "Pre-commit hook: not installed"
    fi

    echo ""
    print_status "INFO" "Hook file location: $hook_file"
    print_status "INFO" "Source hooks directory: $HOOKS_DIR"
}

show_help() {
    echo "CortexDB Git Hooks Setup Script"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  install    Install git hooks (default)"
    echo "  uninstall  Remove git hooks"
    echo "  status     Show current hook status"
    echo "  test       Test hook installation"
    echo "  help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                 # Install hooks"
    echo "  $0 install         # Install hooks"
    echo "  $0 uninstall       # Remove hooks"
    echo "  $0 status          # Check status"
    echo ""
    echo "Environment Variables:"
    echo "  SKIP_TESTS=1       Skip unit tests in pre-commit hook"
    echo ""
}

main() {
    local command="${1:-install}"

    echo "CortexDB Git Hooks Setup"
    echo "========================"
    echo ""

    case "$command" in
        "install")
            check_requirements
            install_hooks
            test_hooks
            echo ""
            print_status "OK" "Git hooks installation completed!"
            echo ""
            echo "The pre-commit hook will now run automatically before each commit."
            echo "It will check code formatting, run tidy checks, and run tests."
            echo ""
            echo "To bypass the hook temporarily, use: git commit --no-verify"
            echo "To disable tests in the hook, set: SKIP_TESTS=1"
            ;;
        "uninstall")
            uninstall_hooks
            echo ""
            print_status "OK" "Git hooks removed"
            ;;
        "status")
            show_status
            ;;
        "test")
            test_hooks
            ;;
        "help"|"-h"|"--help")
            show_help
            ;;
        *)
            echo "Unknown command: $command"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

main "$@"
