#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SQLITE0="$PROJECT_ROOT/zig-out/bin/sqlite0"
CASES_DIR="$SCRIPT_DIR/cases"

# Build sqlite0
echo "Building sqlite0..."
cd "$PROJECT_ROOT"
zig build

if [ ! -f "$SQLITE0" ]; then
    echo "ERROR: sqlite0 binary not found at $SQLITE0"
    exit 1
fi

passed=0
failed=0
total=0

for case_file in "$CASES_DIR"/*.sql; do
    test_name="$(basename "$case_file" .sql)"
    total=$((total + 1))

    # Run with sqlite3 (reference)
    sqlite3_output=$(sqlite3 :memory: < "$case_file" 2>&1 || true)

    # Run with sqlite0 (our implementation)
    # Remove prompt prefix, banner lines, OK lines, and empty lines
    sqlite0_output=$("$SQLITE0" < "$case_file" 2>/dev/null | sed -e ':a' -e 's/^sqlite0> //' -e 'ta' -e ':b' -e 's/^   \.\.\.> //' -e 'tb' | grep -v "^sqlite0 v" | grep -v "^Enter SQL" | grep -v "^Bye" | grep -v "^OK$" | sed '/^$/d' || true)
    # Also strip empty lines from sqlite3 output for consistent NULL handling
    sqlite3_output=$(echo "$sqlite3_output" | sed '/^$/d')

    # Compare
    if [ "$sqlite3_output" = "$sqlite0_output" ]; then
        echo "PASS: $test_name"
        passed=$((passed + 1))
    else
        echo "FAIL: $test_name"
        echo "  sqlite3: $(echo "$sqlite3_output" | head -5)"
        echo "  sqlite0: $(echo "$sqlite0_output" | head -5)"
        failed=$((failed + 1))
    fi
done

echo ""
echo "Results: $passed/$total passed, $failed failed"

if [ "$failed" -gt 0 ]; then
    exit 1
fi
