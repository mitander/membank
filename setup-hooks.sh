#!/bin/sh
#
# Setup script for CortexDB git hooks
#
# This script installs the git hooks required for CortexDB development:
# - pre-commit: Runs code formatting, quality checks, and tests
# - commit-msg: Validates commit message format using Conventional Commits
#

set -e

echo "Setting up CortexDB git hooks..."

# Check if we're in a git repository
if ! git rev-parse --git-dir >/dev/null 2>&1; then
    echo "Error: Not in a git repository"
    exit 1
fi

# Create hooks directory if it doesn't exist
mkdir -p .git/hooks

# Install pre-commit hook
if [ -f ".githooks/pre-commit" ]; then
    cp .githooks/pre-commit .git/hooks/pre-commit
    chmod +x .git/hooks/pre-commit
    echo "✓ Installed pre-commit hook"
else
    echo "Warning: .githooks/pre-commit not found"
fi

# Install commit-msg hook
if [ -f ".githooks/commit-msg" ]; then
    cp .githooks/commit-msg .git/hooks/commit-msg
    chmod +x .git/hooks/commit-msg
    echo "✓ Installed commit-msg hook"
else
    echo "Warning: .githooks/commit-msg not found"
fi

echo ""
echo "Git hooks successfully installed!"
echo ""
echo "The following checks will now run automatically:"
echo "- Pre-commit: Code formatting, quality checks, and tests"
echo "- Commit-msg: Conventional Commits format validation"
echo ""
echo "To bypass hooks (not recommended), use: git commit --no-verify"