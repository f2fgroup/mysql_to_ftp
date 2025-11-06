#!/bin/bash
#
# Test script for mysql_to_sftp.sh
# Tests the core logic without requiring MySQL/SFTP infrastructure
#

set -euo pipefail

echo "=== Testing MySQL to SFTP Script ==="
echo ""

# Test 1: Syntax check
echo "Test 1: Checking script syntax..."
if bash -n mysql_to_sftp.sh; then
    echo "✓ Script syntax is valid"
else
    echo "✗ Script syntax error"
    exit 1
fi
echo ""

# Test 2: Configuration validation (should fail without required vars)
echo "Test 2: Testing configuration validation..."
./mysql_to_sftp.sh > /tmp/test_output.log 2>&1 || true
if grep -q "Configuration validation failed" /tmp/test_output.log; then
    echo "✓ Configuration validation works correctly"
else
    echo "✗ Configuration validation not working as expected"
    cat /tmp/test_output.log
    exit 1
fi
rm -f /tmp/test_output.log
echo ""

# Test 3: Check SQL files are discovered
echo "Test 3: Checking SQL file discovery..."
if [ -f "sql/queries/users_export.sql" ] && [ -f "sql/queries/orders_export.sql" ]; then
    echo "✓ Example SQL files exist"
else
    echo "✗ Example SQL files not found"
    exit 1
fi
echo ""

# Test 4: Verify script is executable
echo "Test 4: Checking script is executable..."
if [ -x "mysql_to_sftp.sh" ]; then
    echo "✓ Script is executable"
else
    echo "✗ Script is not executable"
    exit 1
fi
echo ""

# Test 5: Check example config file exists
echo "Test 5: Checking example configuration file..."
if [ -f "config.env.example" ]; then
    echo "✓ Example configuration file exists"
else
    echo "✗ Example configuration file not found"
    exit 1
fi
echo ""

# Test 6: Verify .gitignore exists and contains important entries
echo "Test 6: Checking .gitignore..."
if [ -f ".gitignore" ] && grep -q ".env" .gitignore && grep -q "*.log" .gitignore; then
    echo "✓ .gitignore configured correctly"
else
    echo "✗ .gitignore missing or incomplete"
    exit 1
fi
echo ""

# Test 7: Check SQL query format (no INTO OUTFILE, proper SELECT)
echo "Test 7: Validating SQL query format..."
sql_has_into_outfile=0
for sql_file in sql/queries/*.sql; do
    if grep -iq "INTO OUTFILE" "$sql_file"; then
        echo "✗ SQL file $sql_file contains INTO OUTFILE (should not)"
        sql_has_into_outfile=1
    fi
    if ! grep -iq "SELECT" "$sql_file"; then
        echo "✗ SQL file $sql_file does not contain SELECT statement"
        sql_has_into_outfile=1
    fi
done

if [ $sql_has_into_outfile -eq 0 ]; then
    echo "✓ SQL files are formatted correctly"
else
    exit 1
fi
echo ""

# Test 8: Verify README has proper documentation
echo "Test 8: Checking README documentation..."
if [ -f "README.md" ] && grep -q "MySQL to SFTP" README.md && grep -q "INTO OUTFILE" README.md; then
    echo "✓ README documentation exists and is comprehensive"
else
    echo "✗ README documentation missing or incomplete"
    exit 1
fi
echo ""

echo "=== All Tests Passed! ==="
echo ""
echo "Note: These are basic validation tests. Full integration testing"
echo "requires actual MySQL and SFTP server infrastructure."
