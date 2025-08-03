#!/bin/bash
#
# KausalDB Git Hooks Setup Script
#
# This script installs and manages git hooks for KausalDB development.
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

    # Check if project zig is available
    if [ ! -x "./zig/zig" ]; then
        print_status "WARN" "Project Zig not found - run './scripts/install_zig.sh' to install"
    else
        print_status "OK" "Project Zig found"
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
    install_hook "pre-commit"

    # Install commit-msg hook
    install_hook "commit-msg"
}

# Helper function to install a specific hook
install_hook() {
    local hook_name="$1"
    local source_hook="$HOOKS_DIR/$hook_name"
    local target_hook="$GIT_HOOKS_DIR/$hook_name"

    if [ ! -f "$source_hook" ]; then
        print_status "FAIL" "Source hook not found: $source_hook"
        exit 1
    fi

    # Backup existing hook if it exists
    if [ -f "$target_hook" ]; then
        local backup_file="$target_hook.backup.$(date +%s)"
        cp "$target_hook" "$backup_file"
        print_status "INFO" "Backed up existing $hook_name hook to: $backup_file"
    fi

    # Copy and make executable
    cp "$source_hook" "$target_hook"
    chmod +x "$target_hook"

    print_status "OK" "$hook_name hook installed"
}

test_hooks() {
    print_status "INFO" "Testing hook installation..."

    # Test pre-commit hook
    test_hook "pre-commit"

    # Test commit-msg hook
    test_hook "commit-msg"
}

# Helper function to test a specific hook
test_hook() {
    local hook_name="$1"
    local hook_file="$GIT_HOOKS_DIR/$hook_name"

    if [ ! -f "$hook_file" ]; then
        print_status "FAIL" "$hook_name hook file not found after installation"
        return 1
    fi

    if [ ! -x "$hook_file" ]; then
        print_status "FAIL" "$hook_name hook file is not executable"
        return 1
    fi

    # Test specific hook functionality
    case "$hook_name" in
        "pre-commit")
            print_status "INFO" "Testing pre-commit hook (fast mode)..."
            if cd "$PROJECT_ROOT" && "$hook_file" >/dev/null 2>&1; then
                print_status "OK" "Pre-commit hook test passed"
            else
                print_status "WARN" "Pre-commit hook test failed - check your development environment"
                print_status "INFO" "Run './zig/zig build tidy' manually to diagnose issues"
            fi
            ;;
        "commit-msg")
            print_status "INFO" "Testing commit-msg hook..."
            # Create a temporary commit message for testing
            local temp_msg=$(mktemp)
            echo "feat(test): sample commit message" > "$temp_msg"
            if cd "$PROJECT_ROOT" && "$hook_file" "$temp_msg" >/dev/null 2>&1; then
                print_status "OK" "Commit-msg hook test passed"
            else
                print_status "WARN" "Commit-msg hook test failed"
            fi
            rm -f "$temp_msg"
            ;;
    esac
}

uninstall_hooks() {
    print_status "INFO" "Uninstalling git hooks..."

    # Remove pre-commit hook
    uninstall_hook "pre-commit"

    # Remove commit-msg hook
    uninstall_hook "commit-msg"
}

# Helper function to uninstall a specific hook
uninstall_hook() {
    local hook_name="$1"
    local hook_file="$GIT_HOOKS_DIR/$hook_name"

    if [ -f "$hook_file" ]; then
        rm "$hook_file"
        print_status "OK" "$hook_name hook removed"
    else
        print_status "INFO" "No $hook_name hook found to remove"
    fi
}

show_status() {
    print_status "INFO" "Git hooks status:"
    echo ""

    # Check pre-commit hook
    show_hook_status "pre-commit"

    # Check commit-msg hook
    show_hook_status "commit-msg"

    echo ""
    print_status "INFO" "Hooks directory: $GIT_HOOKS_DIR"
    print_status "INFO" "Source hooks directory: $HOOKS_DIR"
}

# Helper function to show status of a specific hook
show_hook_status() {
    local hook_name="$1"
    local hook_file="$GIT_HOOKS_DIR/$hook_name"

    if [ -f "$hook_file" ]; then
        if [ -x "$hook_file" ]; then
            print_status "OK" "$hook_name hook: installed and executable"
        else
            print_status "WARN" "$hook_name hook: installed but not executable"
        fi
    else
        print_status "FAIL" "$hook_name hook: not installed"
    fi
}

show_help() {
    echo "KausalDB Git Hooks Setup Script"
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
    echo "  RUN_TESTS=1        Enable unit tests in pre-commit hook (disabled by default)"
    echo ""
}

main() {
    local command="${1:-install}"

    echo "KausalDB Git Hooks Setup"
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
            echo "Git hooks will now run automatically:"
            echo "  - Pre-commit: fast checks (formatting, tidy) - tests skipped by default"
            echo "  - Commit-msg: validates conventional commit format"
            echo ""
            echo "To bypass hooks temporarily, use: git commit --no-verify"
            echo "To run tests in pre-commit hook, set: RUN_TESTS=1"
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
