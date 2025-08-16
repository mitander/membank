#!/usr/bin/env bash
#
# KausalDB Development Environment Setup
#
# This script sets up everything needed for KausalDB development:
# - Downloads and installs the correct Zig version
# - Configures Git hooks for code quality
# - Validates the development environment
#
# Usage: ./scripts/setup.sh [zig_version]
#
# If no zig_version is provided, uses the project default.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo -e "${BLUE}KausalDB Development Environment Setup${NC}"
echo "=================================================="
echo ""

# Step 1: Install Zig
echo -e "${YELLOW}Step 1: Installing Zig toolchain...${NC}"
ZIG_VERSION=${1:-}
if [ -n "$ZIG_VERSION" ]; then
    echo "Installing Zig version: $ZIG_VERSION"
    "$SCRIPT_DIR/install_zig.sh" "$ZIG_VERSION"
else
    echo "Installing default Zig version (as specified in build.zig.zon)"
    "$SCRIPT_DIR/install_zig.sh"
fi

if [ $? -eq 0 ]; then
    echo -e "${GREEN}[OK] Zig installation completed${NC}"
else
    echo -e "${RED}[FAIL] Zig installation failed${NC}"
    exit 1
fi

echo ""

# Step 2: Setup Git hooks
echo -e "${YELLOW}Step 2: Setting up Git hooks...${NC}"
"$SCRIPT_DIR/setup_hooks.sh"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}[OK] Git hooks configured${NC}"
else
    echo -e "${RED}[FAIL] Git hooks setup failed${NC}"
    exit 1
fi

echo ""

# Step 3: Validate environment
echo -e "${YELLOW}Step 3: Validating development environment...${NC}"

# Check Zig installation
if [ -x "$PROJECT_ROOT/zig/zig" ]; then
    ZIG_VERSION_OUTPUT=$("$PROJECT_ROOT/zig/zig" version)
    echo "Zig version: $ZIG_VERSION_OUTPUT"
else
    echo -e "${RED}[FAIL] Zig not found at expected location${NC}"
    exit 1
fi

# Check Git hooks
if [ -f "$PROJECT_ROOT/.git/hooks/pre-commit" ]; then
    echo "Git pre-commit hook: installed"
else
    echo -e "${RED}[FAIL] Git pre-commit hook not found${NC}"
    exit 1
fi

# Quick build test
echo "Testing build system..."
cd "$PROJECT_ROOT"
if "$PROJECT_ROOT/zig/zig" build --summary all > /dev/null 2>&1; then
    echo -e "${GREEN}[OK] Build system working${NC}"
else
    echo -e "${RED}[FAIL] Build test failed${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}Setup completed successfully!${NC}"
echo ""
echo "Next steps:"
echo "  1. Run tests:           ./zig/zig build test"
echo "  2. Run full test suite: ./zig/zig build test-all"
echo "  3. Start development:   ./zig/zig build run"
echo ""
echo "For more information, see the README.md file."