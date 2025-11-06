#!/bin/bash
#
# Demo script showing how SQL queries are transformed
# This demonstrates the INTO OUTFILE clause construction without requiring MySQL
#

set -euo pipefail

echo "=== MySQL to SFTP Query Transformation Demo ==="
echo ""
echo "This script demonstrates how the tool transforms SQL queries"
echo "by adding the INTO OUTFILE clause."
echo ""

# Function to simulate the transformation
transform_query() {
    local sql_file="$1"
    local output_file="$2"
    
    echo "Processing: $sql_file"
    echo "Output will be: $output_file"
    echo ""
    
    # Read and clean query
    local query=$(cat "$sql_file" | sed 's/[[:space:]]*;[[:space:]]*$//')
    
    echo "--- Original Query ---"
    echo "$query"
    echo ""
    
    # Add INTO OUTFILE clause
    echo "--- Transformed Query (with INTO OUTFILE) ---"
    cat <<EOF
SET NAMES utf8mb4;

${query}
INTO OUTFILE '$output_file'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"'
LINES TERMINATED BY '\n';
EOF
    echo ""
    echo "--- CSV Format Explanation ---"
    echo "• Field separator: comma (,)"
    echo "• Field enclosure: double quote (\")"
    echo "• Escape character: double quote (\")"
    echo "• Line terminator: newline (\\n)"
    echo "• Character encoding: UTF-8 (utf8mb4)"
    echo ""
    echo "--- SFTP Upload Process ---"
    echo "1. Upload as: ${output_file##*/}.part"
    echo "2. Rename to: ${output_file##*/}"
    echo "   (atomic operation - file appears only when complete)"
    echo ""
}

# Demo with example queries
if [ -d "sql/queries" ]; then
    for sql_file in sql/queries/*.sql; do
        if [ -f "$sql_file" ]; then
            basename=$(basename "$sql_file" .sql)
            output_file="/var/lib/mysql-files/${basename}.csv"
            transform_query "$sql_file" "$output_file"
            echo "=================================================="
            echo ""
        fi
    done
else
    echo "Error: sql/queries directory not found"
    exit 1
fi

echo "=== Demo Complete ==="
echo ""
echo "To use the actual script:"
echo "1. Configure your environment (copy config.env.example to .env)"
echo "2. Set up MySQL with FILE privilege and secure_file_priv"
echo "3. Configure SFTP credentials"
echo "4. Run: ./mysql_to_sftp.sh"
