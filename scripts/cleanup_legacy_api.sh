#!/bin/bash
# Cleanup Legacy Next.js API Routes
#
# This script removes the legacy Next.js API routes that are no longer needed
# after the frontend/backend separation refactoring.
#
# Usage: ./scripts/cleanup_legacy_api.sh
#
# The frontend components now use the Python backend API directly via
# the /src/lib/api.ts utility library.

set -e

DASHBOARD_DIR="$(cd "$(dirname "$0")/.." && pwd)/dashboard"

echo "=== Cleanup Legacy Next.js API Routes ==="
echo "Dashboard directory: $DASHBOARD_DIR"
echo ""

# List of files/directories to remove
LEGACY_FILES=(
    "src/app/api"
    "src/lib/db.ts"
)

echo "The following legacy files will be removed:"
for item in "${LEGACY_FILES[@]}"; do
    full_path="$DASHBOARD_DIR/$item"
    if [ -e "$full_path" ]; then
        echo "  - $item"
    else
        echo "  - $item (already removed)"
    fi
done

echo ""
read -p "Do you want to proceed? (y/N) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    for item in "${LEGACY_FILES[@]}"; do
        full_path="$DASHBOARD_DIR/$item"
        if [ -e "$full_path" ]; then
            rm -rf "$full_path"
            echo "Removed: $item"
        fi
    done

    echo ""
    echo "=== Cleanup Complete ==="
    echo ""
    echo "Next steps:"
    echo "1. Run 'npm install' in the dashboard directory"
    echo "2. Run 'npm run build' to verify the build succeeds"
    echo "3. Test the application with 'npm run dev'"
else
    echo "Cleanup cancelled."
    exit 0
fi
