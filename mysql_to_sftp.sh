#!/bin/bash
#
# MySQL to SFTP Extraction Script
# Executes SQL queries from .sql files, generates CSV files using MySQL's INTO OUTFILE,
# and uploads them to an SFTP server using atomic operations.
#
# Requirements:
# - bash, mysql client, sftp
# - MySQL user with FILE privilege
# - MySQL secure_file_priv configured and accessible
#

set -euo pipefail

# Default configuration
SQL_DIR="${SQL_DIR:-sql/queries}"
OUTPUT_DIR="${OUTPUT_DIR:-/var/lib/mysql-files}"
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
MYSQL_DATABASE="${MYSQL_DATABASE:-}"
SFTP_HOST="${SFTP_HOST:-}"
SFTP_PORT="${SFTP_PORT:-22}"
SFTP_USER="${SFTP_USER:-}"
SFTP_PASSWORD="${SFTP_PASSWORD:-}"
SFTP_REMOTE_DIR="${SFTP_REMOTE_DIR:-/upload}"
SFTP_KEY_FILE="${SFTP_KEY_FILE:-}"
LOG_FILE="${LOG_FILE:-/tmp/mysql_to_sftp.log}"

# CSV format settings (as per specification)
CSV_FIELD_TERMINATOR=','
CSV_FIELD_ENCLOSURE='"'
CSV_ESCAPE_CHAR='"'
CSV_LINE_TERMINATOR='\n'

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# Function to log error messages
log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$LOG_FILE" >&2
}

# Function to validate required parameters
validate_config() {
    local errors=0
    
    if [[ -z "$MYSQL_USER" ]]; then
        log_error "MYSQL_USER is required"
        errors=$((errors + 1))
    fi
    
    if [[ -z "$MYSQL_DATABASE" ]]; then
        log_error "MYSQL_DATABASE is required"
        errors=$((errors + 1))
    fi
    
    if [[ -z "$SFTP_HOST" ]]; then
        log_error "SFTP_HOST is required"
        errors=$((errors + 1))
    fi
    
    if [[ -z "$SFTP_USER" ]]; then
        log_error "SFTP_USER is required"
        errors=$((errors + 1))
    fi
    
    if [[ -z "$SFTP_PASSWORD" ]] && [[ -z "$SFTP_KEY_FILE" ]]; then
        log_error "Either SFTP_PASSWORD or SFTP_KEY_FILE is required"
        errors=$((errors + 1))
    fi
    
    if [[ ! -d "$SQL_DIR" ]]; then
        log_error "SQL directory does not exist: $SQL_DIR"
        errors=$((errors + 1))
    fi
    
    if [[ $errors -gt 0 ]]; then
        return 1
    fi
    
    return 0
}

# Function to clean SQL query
clean_sql_query() {
    local sql_file="$1"
    local query
    
    # Read the SQL file and remove trailing semicolon and whitespace
    query=$(cat "$sql_file" | sed 's/[[:space:]]*;[[:space:]]*$//')
    
    echo "$query"
}

# Function to check if query already contains INTO OUTFILE
check_into_outfile() {
    local query="$1"
    
    if echo "$query" | grep -iq "INTO OUTFILE"; then
        return 0  # Already has INTO OUTFILE
    else
        return 1  # Does not have INTO OUTFILE
    fi
}

# Function to build INTO OUTFILE clause
build_into_outfile_clause() {
    local output_file="$1"
    
    cat <<EOF
INTO OUTFILE '$output_file'
FIELDS TERMINATED BY '$CSV_FIELD_TERMINATOR' OPTIONALLY ENCLOSED BY '$CSV_FIELD_ENCLOSURE' ESCAPED BY '$CSV_ESCAPE_CHAR'
LINES TERMINATED BY '$CSV_LINE_TERMINATOR'
EOF
}

# Function to execute MySQL query with INTO OUTFILE
execute_query() {
    local sql_query="$1"
    local output_file="$2"
    
    # Remove the output file if it already exists (MySQL won't overwrite)
    if [[ -f "$output_file" ]]; then
        log "Removing existing output file: $output_file"
        rm -f "$output_file"
    fi
    
    # Build the complete SQL statement
    local into_clause
    into_clause=$(build_into_outfile_clause "$output_file")
    
    local complete_query="${sql_query}
${into_clause};"
    
    log "Executing query to generate: $output_file"
    
    # Execute the query
    local mysql_cmd="mysql"
    mysql_cmd+=" -h${MYSQL_HOST}"
    mysql_cmd+=" -P${MYSQL_PORT}"
    mysql_cmd+=" -u${MYSQL_USER}"
    mysql_cmd+=" ${MYSQL_DATABASE}"
    mysql_cmd+=" -e"
    
    # Set UTF-8 encoding
    local full_query="SET NAMES utf8mb4; ${complete_query}"
    
    # Execute with password passed via stdin to avoid command line exposure
    if [[ -n "$MYSQL_PASSWORD" ]]; then
        if ! echo "$full_query" | mysql --defaults-extra-file=<(cat <<EOF
[client]
password=${MYSQL_PASSWORD}
EOF
) -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" "${MYSQL_DATABASE}" -e "$(cat)"; then
            log_error "Failed to execute query"
            return 1
        fi
    else
        if ! echo "$full_query" | $mysql_cmd; then
            log_error "Failed to execute query"
            return 1
        fi
    fi
    
    # Verify the output file was created
    if [[ ! -f "$output_file" ]]; then
        log_error "Output file was not created: $output_file"
        return 1
    fi
    
    log "Successfully generated CSV file: $output_file"
    return 0
}

# Function to upload file to SFTP using atomic operation
upload_to_sftp() {
    local local_file="$1"
    local remote_filename="$2"
    
    local remote_temp="${remote_filename}.part"
    local remote_final="${SFTP_REMOTE_DIR}/${remote_filename}"
    local remote_temp_full="${SFTP_REMOTE_DIR}/${remote_temp}"
    
    log "Uploading file to SFTP: $local_file -> $remote_final"
    
    # Create SFTP batch commands
    local sftp_batch=$(mktemp)
    cat > "$sftp_batch" <<EOF
cd $SFTP_REMOTE_DIR
put $local_file $remote_temp
rename $remote_temp $remote_filename
bye
EOF
    
    # Execute SFTP commands
    local sftp_cmd="sftp"
    sftp_cmd+=" -P ${SFTP_PORT}"
    sftp_cmd+=" -b ${sftp_batch}"
    
    if [[ -n "$SFTP_KEY_FILE" ]]; then
        sftp_cmd+=" -i ${SFTP_KEY_FILE}"
    fi
    
    sftp_cmd+=" ${SFTP_USER}@${SFTP_HOST}"
    
    # Execute with password if provided and no key file
    if [[ -n "$SFTP_PASSWORD" ]] && [[ -z "$SFTP_KEY_FILE" ]]; then
        if ! command -v sshpass &> /dev/null; then
            log_error "sshpass is required for password authentication but not installed"
            rm -f "$sftp_batch"
            return 1
        fi
        # Use SSHPASS environment variable to avoid password in process list
        export SSHPASS="$SFTP_PASSWORD"
        sftp_cmd="sshpass -e ${sftp_cmd}"
    fi
    
    if eval "$sftp_cmd"; then
        log "Successfully uploaded: $remote_final"
        rm -f "$sftp_batch"
        unset SSHPASS
        return 0
    else
        log_error "Failed to upload file to SFTP"
        rm -f "$sftp_batch"
        unset SSHPASS
        return 1
    fi
}

# Function to process a single SQL file
process_sql_file() {
    local sql_file="$1"
    local basename
    basename=$(basename "$sql_file" .sql)
    local csv_filename="${basename}.csv"
    local output_file="${OUTPUT_DIR}/${csv_filename}"
    
    log "Processing SQL file: $sql_file"
    
    # Clean the SQL query
    local query
    query=$(clean_sql_query "$sql_file")
    
    if [[ -z "$query" ]]; then
        log_error "Empty query in file: $sql_file"
        return 1
    fi
    
    # Check if query already contains INTO OUTFILE
    if check_into_outfile "$query"; then
        log_error "Query already contains INTO OUTFILE clause: $sql_file"
        return 1
    fi
    
    # Execute query to generate CSV
    if ! execute_query "$query" "$output_file"; then
        log_error "Failed to execute query for: $sql_file"
        return 1
    fi
    
    # Upload to SFTP
    if ! upload_to_sftp "$output_file" "$csv_filename"; then
        log_error "Failed to upload CSV for: $sql_file"
        return 1
    fi
    
    # Clean up local CSV file (optional)
    # Uncomment the following line if you want to remove local files after upload
    # rm -f "$output_file"
    
    log "Successfully processed: $sql_file"
    return 0
}

# Main execution
main() {
    log "=== MySQL to SFTP Extraction Script Started ==="
    
    # Validate configuration
    if ! validate_config; then
        log_error "Configuration validation failed"
        exit 1
    fi
    
    log "Configuration validated successfully"
    log "SQL directory: $SQL_DIR"
    log "Output directory: $OUTPUT_DIR"
    log "MySQL database: $MYSQL_DATABASE"
    log "SFTP host: $SFTP_HOST"
    
    # Find all .sql files
    local sql_files
    sql_files=$(find "$SQL_DIR" -type f -name "*.sql" | sort)
    
    if [[ -z "$sql_files" ]]; then
        log "No SQL files found in: $SQL_DIR"
        exit 0
    fi
    
    local total_files
    total_files=$(echo "$sql_files" | wc -l)
    log "Found $total_files SQL file(s) to process"
    
    # Process each SQL file
    local success_count=0
    local error_count=0
    
    while IFS= read -r sql_file; do
        if process_sql_file "$sql_file"; then
            success_count=$((success_count + 1))
        else
            error_count=$((error_count + 1))
        fi
    done <<< "$sql_files"
    
    log "=== MySQL to SFTP Extraction Script Completed ==="
    log "Total files: $total_files"
    log "Successful: $success_count"
    log "Errors: $error_count"
    
    if [[ $error_count -gt 0 ]]; then
        exit 1
    fi
    
    exit 0
}

# Run main function
main "$@"
