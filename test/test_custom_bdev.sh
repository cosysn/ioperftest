#!/bin/bash
# SPDX-License-Identifier: BSD-3-Clause
# Test script for Custom SPDK Bdev

set -e

echo "========================================"
echo "Custom SPDK Bdev Test Script"
echo "========================================"

# Set defaults
SPDK_ROOT=${SPDK_ROOT:-/opt/spdk}
MAX_IOPS=${MAX_IOPS:-0}
MAX_BW=${MAX_BW:-0}

# Build the module
echo "Building custom bdev module..."
cd /home/ubuntu/ioperftest

# Check if we can build with SPDK
if [ -f "$SPDK_ROOT/build/lib/libspdk.so" ]; then
    echo "Using SPDK from: $SPDK_ROOT"
    make clean
    make SPDKBUILD=$SPDK_ROOT
else
    echo "SPDK not built, building test version..."
    make clean
    make
fi

echo ""
echo "Build complete!"
echo ""

# Show help if no arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --max-iops=N    Set maximum IOPS (0 = unlimited)"
    echo "  --max-bw=N      Set maximum bandwidth in MB/s (0 = unlimited)"
    echo ""
    echo "Example:"
    echo "  MAX_IOPS=100000 $0"
    echo "  MAX_BW=500 $0 --max-iops=50000"
    exit 0
fi

# Run the test application
echo "Starting test application..."
exec ./custom_bdev_test "$@"
