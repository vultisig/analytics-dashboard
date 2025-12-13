#!/bin/bash
# Cleanup unused files from the repository

set -e

echo "🧹 Cleaning up unused files..."
echo ""

# Track what we're removing
removed_count=0

# Function to remove file/directory if it exists
safe_remove() {
    if [ -e "$1" ]; then
        echo "  ✗ Removing: $1"
        rm -rf "$1"
        ((removed_count++))
    fi
}

echo "1️⃣  Removing debug/test files from dashboard..."
safe_remove "dashboard/debug_1inch_chains.ts"
safe_remove "dashboard/check_midgard_fields.js"
safe_remove "dashboard/debug_db.ts"
safe_remove "dashboard/check_new_fields.js"
safe_remove "dashboard/investigate_outlier.js"
safe_remove "dashboard/test_date_filtering.js"
safe_remove "dashboard/test_revenue_api_debug.js"
safe_remove "dashboard/check_data.js"
safe_remove "dashboard/test_date_filter.js"

echo ""
echo "2️⃣  Removing log files..."
safe_remove "vultisig-analytics/sync.log"
safe_remove "dashboard/.next/dev/logs"

echo ""
echo "3️⃣  Removing macOS system files..."
safe_remove ".DS_Store"
safe_remove "vultisig-analytics/.DS_Store"
safe_remove "dashboard/.DS_Store"

echo ""
echo "4️⃣  Removing Python cache..."
safe_remove "vultisig-analytics/__pycache__"
find vultisig-analytics -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find vultisig-analytics -type f -name "*.pyc" -delete 2>/dev/null || true

echo ""
echo "5️⃣  Removing unused Node.js files from Python backend..."
safe_remove "vultisig-analytics/package.json"
safe_remove "vultisig-analytics/package-lock.json"
safe_remove "vultisig-analytics/node_modules"

echo ""
echo "6️⃣  Removing Next.js build artifacts (will be rebuilt)..."
safe_remove "dashboard/.next"

echo ""
echo "✨ Cleanup complete!"
echo "📊 Removed $removed_count items"
echo ""
echo "💡 Note: .gitignore has been updated to prevent these files from being committed in the future."
