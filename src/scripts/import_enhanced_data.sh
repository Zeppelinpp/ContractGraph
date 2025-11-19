#!/bin/bash

# Enhanced Data Import Script
# This script generates graph data from enhanced_mock_data and imports it to Nebula Graph

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"

echo "======================================"
echo "Enhanced Data Import Pipeline"
echo "======================================"
echo ""

# Step 1: Generate graph data
echo "Step 1/2: Generating graph data from enhanced_mock_data..."
echo "--------------------------------------"
uv run python src/scripts/generate_enhanced_graph_data.py

if [ $? -ne 0 ]; then
    echo "❌ Failed to generate graph data"
    exit 1
fi

echo ""
echo "✓ Graph data generated successfully"
echo ""

# Step 2: Import to Nebula Graph
echo "Step 2/2: Importing to Nebula Graph..."
echo "--------------------------------------"
uv run python src/scripts/nebula_import.py --data-dir enhanced_graph_data

if [ $? -ne 0 ]; then
    echo "❌ Failed to import data to Nebula Graph"
    exit 1
fi

echo ""
echo "======================================"
echo "✓ Import pipeline completed successfully"
echo "======================================"

