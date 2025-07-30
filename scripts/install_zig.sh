#!/usr/bin/env sh
set -eu

# Default to the specific dev version required by Membank
ZIG_RELEASE_DEFAULT="0.15.0-dev.1108+27212a3e6"
ZIG_RELEASE=${1:-$ZIG_RELEASE_DEFAULT}

# Get the project root directory (parent of scripts)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Validate the release version and determine download strategy
if [ "$ZIG_RELEASE" = "latest" ]; then
    ZIG_JSON_KEY="master"
    USE_INDEX_JSON=true
    echo "Downloading Zig latest development build..."
elif echo "$ZIG_RELEASE" | grep -q '^[0-9]\+\.[0-9]\+\.[0-9]\+$'; then
    ZIG_JSON_KEY="$ZIG_RELEASE"
    USE_INDEX_JSON=true
    echo "Downloading Zig $ZIG_RELEASE release build..."
elif echo "$ZIG_RELEASE" | grep -q '^[0-9]\+\.[0-9]\+\.[0-9]\+-dev\.'; then
    # Handle dev versions like 0.15.0-dev.1108+27212a3e6
    USE_INDEX_JSON=false
    echo "Downloading Zig $ZIG_RELEASE development build..."
else
    echo "Release version invalid. Use 'latest', a version like '0.13.0', or a dev version like '0.15.0-dev.1108+27212a3e6'"
    exit 1
fi

# Determine the architecture:
if [ "$(uname -m)" = 'arm64' ] || [ "$(uname -m)" = 'aarch64' ]; then
    ZIG_ARCH="aarch64"
else
    ZIG_ARCH="x86_64"
fi

# Determine the operating system:
case "$(uname)" in
    Linux)
        ZIG_OS="linux"
        ;;
    Darwin)
        ZIG_OS="macos"
        ;;
    CYGWIN*|MINGW*|MSYS*)
        ZIG_OS="windows"
        ;;
    *)
        echo "Unknown OS: $(uname)"
        exit 1
        ;;
esac

ZIG_TARGET="$ZIG_ARCH-$ZIG_OS"

if [ "$USE_INDEX_JSON" = true ]; then
    # Use the JSON index for stable releases and latest
    echo "Fetching Zig download index..."
    if command -v wget > /dev/null; then
        ipv4="-4"
        if [ -f /etc/alpine-release ]; then
            ipv4=""
        fi
        # shellcheck disable=SC2086
        JSON_DATA=$(wget $ipv4 --quiet -O - https://ziglang.org/download/index.json)
    else
        JSON_DATA=$(curl --silent https://ziglang.org/download/index.json)
    fi

    # Extract the tarball URL
    ZIG_URL=""
    if command -v jq > /dev/null; then
        ZIG_URL=$(echo "$JSON_DATA" | jq -r ".[\"$ZIG_JSON_KEY\"][\"$ZIG_TARGET\"].tarball // empty")
    elif command -v python3 > /dev/null; then
        ZIG_URL=$(echo "$JSON_DATA" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    url = data['$ZIG_JSON_KEY']['$ZIG_TARGET']['tarball']
    print(url)
except:
    pass
")
    else
        # Fall back to grep/sed approach
        ZIG_URL=$(echo "$JSON_DATA" | grep -A 20 "\"$ZIG_JSON_KEY\":" | grep -A 5 "\"$ZIG_TARGET\":" | grep "tarball" | head -1 | sed 's/.*"tarball":\s*"\([^"]*\)".*/\1/')
    fi

    if [ -z "$ZIG_URL" ]; then
        echo "Release not found on ziglang.org for $ZIG_JSON_KEY/$ZIG_TARGET"
        echo "Available versions can be found at: https://ziglang.org/download/"
        exit 1
    fi
else
    # For dev versions, construct the URL directly
    ZIG_URL="https://ziglang.org/builds/zig-$ZIG_TARGET-$ZIG_RELEASE.tar.xz"
fi

echo "Found download URL: $ZIG_URL"

# Work out the filename from the URL, as well as the directory without the file extension:
ZIG_ARCHIVE=$(basename "$ZIG_URL")

case "$ZIG_ARCHIVE" in
    *".tar.xz")
        ZIG_ARCHIVE_EXT=".tar.xz"
        ;;
    *".zip")
        ZIG_ARCHIVE_EXT=".zip"
        ;;
    *)
        echo "Unknown archive extension for $ZIG_ARCHIVE"
        exit 1
        ;;
esac

ZIG_DIRECTORY=$(basename "$ZIG_ARCHIVE" "$ZIG_ARCHIVE_EXT")

# Create zig directory in project root
ZIG_DIR="$PROJECT_ROOT/zig"
mkdir -p "$ZIG_DIR"

# Download
echo "Downloading $ZIG_URL..."
cd "$ZIG_DIR"
if command -v wget > /dev/null; then
    ipv4="-4"
    if [ -f /etc/alpine-release ]; then
        ipv4=""
    fi
    # shellcheck disable=SC2086
    wget $ipv4 --quiet --output-document="$ZIG_ARCHIVE" "$ZIG_URL"
else
    curl --silent --output "$ZIG_ARCHIVE" "$ZIG_URL"
fi

# Extract and then remove the downloaded archive:
echo "Extracting $ZIG_ARCHIVE..."
case "$ZIG_ARCHIVE_EXT" in
    ".tar.xz")
        tar -xf "$ZIG_ARCHIVE"
        ;;
    ".zip")
        unzip -q "$ZIG_ARCHIVE"
        ;;
    *)
        echo "Unexpected error"
        exit 1
        ;;
esac
rm "$ZIG_ARCHIVE"

# Replace existing directories and files
rm -rf doc lib zig zig.exe LICENSE README.md

# Move the contents from the extracted directory
if [ -f "$ZIG_DIRECTORY/LICENSE" ]; then
    mv "$ZIG_DIRECTORY/LICENSE" ./
fi
if [ -f "$ZIG_DIRECTORY/README.md" ]; then
    mv "$ZIG_DIRECTORY/README.md" ./
fi
if [ -d "$ZIG_DIRECTORY/doc" ]; then
    mv "$ZIG_DIRECTORY/doc" ./
fi
if [ -d "$ZIG_DIRECTORY/lib" ]; then
    mv "$ZIG_DIRECTORY/lib" ./
fi
if [ -f "$ZIG_DIRECTORY/zig" ] || [ -f "$ZIG_DIRECTORY/zig.exe" ]; then
    if [ -f "$ZIG_DIRECTORY/zig" ]; then
        mv "$ZIG_DIRECTORY/zig" ./
    else
        mv "$ZIG_DIRECTORY/zig.exe" ./
    fi
fi

# Remove the now-empty extracted directory
rmdir "$ZIG_DIRECTORY"

# Go back to project root
cd "$PROJECT_ROOT"

# Check if zig binary exists and is executable
ZIG_BIN="$ZIG_DIR/zig"
if [ -f "$ZIG_BIN" ]; then
    chmod +x "$ZIG_BIN"
    echo "Download completed successfully!"
    echo "Zig binary: $ZIG_BIN"
    echo "Version: $($ZIG_BIN version)"
    echo ""
    echo "Add to PATH with: export PATH=\"$ZIG_DIR:\$PATH\""
elif [ -f "$ZIG_DIR/zig.exe" ]; then
    echo "Download completed successfully!"
    echo "Zig binary: $ZIG_DIR/zig.exe"
    echo "Add to PATH manually or use full path to executable"
else
    echo "Warning: zig binary not found after extraction"
    exit 1
fi
