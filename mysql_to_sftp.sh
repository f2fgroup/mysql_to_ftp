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

# --- Config loading: support --config or auto-load .env/config.env ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE=""

# Parse only --config; ignore other args (script itself doesn't use CLI args)
for arg in "$@"; do
    case "$arg" in
        --config=*) CONFIG_FILE="${arg#*=}" ;;
        --config) shift; CONFIG_FILE="${1:-}" ;;
    esac
done

# Decide env file to source
if [[ -z "${CONFIG_FILE}" ]]; then
    if [[ -f "${SCRIPT_DIR}/.env" ]]; then
        CONFIG_FILE="${SCRIPT_DIR}/.env"
    elif [[ -f "${SCRIPT_DIR}/config.env" ]]; then
        CONFIG_FILE="${SCRIPT_DIR}/config.env"
    else
        CONFIG_FILE=""
    fi
fi

if [[ -n "${CONFIG_FILE}" && -f "${CONFIG_FILE}" ]]; then
    # shellcheck disable=SC1090
    set -a; source "${CONFIG_FILE}"; set +a
fi

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
SFTP_DISABLE_HOST_KEY_CHECKING="${SFTP_DISABLE_HOST_KEY_CHECKING:-false}"
SFTP_KNOWN_HOSTS="${SFTP_KNOWN_HOSTS:-}"

# Ensure sshpass is available when using password authentication for SFTP
ensure_sshpass_if_needed() {
    # Only needed when using password (no key file)
    if [[ -n "${SFTP_PASSWORD}" && -z "${SFTP_KEY_FILE}" ]]; then
        if command -v sshpass >/dev/null 2>&1; then
            return 0
        fi
        log "sshpass is required for SFTP password authentication but is not installed."
        if [[ -t 0 ]]; then
            read -r -p "Install sshpass now? [Y/n]: " RESP
            RESP=${RESP:-Y}
            if [[ "${RESP}" =~ ^[Yy]$ ]]; then
                if command -v apt-get >/dev/null 2>&1; then
                    if command -v sudo >/dev/null 2>&1; then
                        if ! sudo -n env DEBIAN_FRONTEND=noninteractive apt-get update -y || \
                           ! sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y sshpass; then
                            log_error "Automatic installation of sshpass failed. Please install it manually and re-run."
                            return 1
                        fi
                    else
                        if ! env DEBIAN_FRONTEND=noninteractive apt-get update -y || \
                           ! env DEBIAN_FRONTEND=noninteractive apt-get install -y sshpass; then
                            log_error "Automatic installation of sshpass failed. Please install it manually and re-run."
                            return 1
                        fi
                    fi
                    log "sshpass installed successfully."
                    return 0
                else
                    log_error "Package manager not found. Please install 'sshpass' manually and re-run."
                    return 1
                fi
            else
                log_error "sshpass is required for password-based SFTP. Aborting as requested."
                return 1
            fi
        else
            log_error "Non-interactive session and sshpass missing. Please install 'sshpass' or use SFTP_KEY_FILE."
            return 1
        fi
    fi
    return 0
}

# Helper to check secure_file_priv and ensure OUTPUT_DIR is writable by MySQL
get_secure_file_priv() {
    local value=""
    if [[ -n "${MYSQL_PASSWORD}" ]]; then
        value=$(mysql --defaults-extra-file=<(printf "[client]\npassword=%s\n" "${MYSQL_PASSWORD}") \
            -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -N -s \
            -e "SHOW VARIABLES LIKE 'secure_file_priv';" 2>/dev/null | awk 'NR==1{next} {print $2}' | tail -n1)
    else
        value=$(mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -N -s \
            -e "SHOW VARIABLES LIKE 'secure_file_priv';" 2>/dev/null | awk 'NR==1{next} {print $2}' | tail -n1)
    fi
    echo "${value}"
}

ensure_output_dir_permissions() {
    local dir="$1"
    # Create directory if missing using sudo when available
    if [[ ! -d "${dir}" ]]; then
        if command -v sudo >/dev/null 2>&1; then
            sudo -n mkdir -p "${dir}" || mkdir -p "${dir}"
        else
            mkdir -p "${dir}"
        fi
    fi
    # Ensure ownership to mysql and secure permissions
    if command -v sudo >/dev/null 2>&1; then
        sudo -n chown mysql:mysql "${dir}" 2>/dev/null || true
        sudo -n chmod 750 "${dir}" 2>/dev/null || true
    else
        chown mysql:mysql "${dir}" 2>/dev/null || true
        chmod 750 "${dir}" 2>/dev/null || true
    fi
}

# Prepare host key verification options for sftp
prepare_host_key_options() {
    local opts=""
    SFTP_KNOWN_HOSTS_TEMP=""
    if [[ "${SFTP_DISABLE_HOST_KEY_CHECKING}" == "true" ]]; then
        log "Host key verification is DISABLED (StrictHostKeyChecking=no)" >/dev/null
        opts+=" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o GlobalKnownHostsFile=/dev/null"
        echo "$opts"
        return 0
    fi

    # Strict host key checking
    if [[ -n "${SFTP_KNOWN_HOSTS}" && -f "${SFTP_KNOWN_HOSTS}" ]]; then
        log "Using provided known_hosts file: ${SFTP_KNOWN_HOSTS}" >/dev/null
        opts+=" -o StrictHostKeyChecking=yes -o UserKnownHostsFile=${SFTP_KNOWN_HOSTS}"
        echo "$opts"
        return 0
    fi

    # Generate a temporary known_hosts file via ssh-keyscan
    if ! command -v ssh-keyscan >/dev/null 2>&1; then
        log_error "ssh-keyscan not found. Install openssh-client or set SFTP_DISABLE_HOST_KEY_CHECKING=true or provide SFTP_KNOWN_HOSTS."
        return 1
    fi
    SFTP_KNOWN_HOSTS_TEMP="$(mktemp)"
    if ! ssh-keyscan -p "${SFTP_PORT}" -T 5 "${SFTP_HOST}" >"${SFTP_KNOWN_HOSTS_TEMP}" 2>/dev/null; then
        log_error "Failed to retrieve host key from ${SFTP_HOST}:${SFTP_PORT}."
        rm -f "${SFTP_KNOWN_HOSTS_TEMP}"
        SFTP_KNOWN_HOSTS_TEMP=""
        return 1
    fi
    log "Using generated known_hosts for ${SFTP_HOST}:${SFTP_PORT}" >/dev/null
    opts+=" -o StrictHostKeyChecking=yes -o UserKnownHostsFile=${SFTP_KNOWN_HOSTS_TEMP}"
    echo "$opts"
    return 0
}

# Run an sftp batch with prepared options; expects hostkey_opts in "$1" and batch content in "$2"
run_sftp_batch() {
    local hostkey_opts="$1"
    local batch_content="$2"

    # Convert hostkey opts string to array tokens
    local extra_opts=()
    if [[ -n "$hostkey_opts" ]]; then
        # shellcheck disable=SC2206
        extra_opts=($hostkey_opts)
    fi

    local cmd=( sftp )
    cmd+=( "${extra_opts[@]}" )
    if [[ -n "$SFTP_PASSWORD" && -z "$SFTP_KEY_FILE" ]]; then
        cmd+=( -o PreferredAuthentications=password -o PubkeyAuthentication=no -o BatchMode=no -o NumberOfPasswordPrompts=1 )
    fi
    cmd+=( -P "${SFTP_PORT}" -b - )
    if [[ -n "$SFTP_KEY_FILE" ]]; then
        cmd+=( -i "${SFTP_KEY_FILE}" )
    fi
    cmd+=( "${SFTP_USER}@${SFTP_HOST}" )

    if [[ "${DEBUG_SFTP:-false}" == "true" ]]; then
        {
            printf "[debug] sftp cmd: ";
            printf "%q " "${cmd[@]}";
            printf "\n";
        } | tee -a "$LOG_FILE" >/dev/null
    fi

    local status=0
    local out
    if [[ -n "$SFTP_PASSWORD" && -z "$SFTP_KEY_FILE" ]]; then
        if command -v sshpass >/dev/null 2>&1; then
            out=$(SSHPASS="$SFTP_PASSWORD" sshpass -e "${cmd[@]}" <<< "$batch_content" 2>&1) || status=$?
        else
            log_error "sshpass not found for password authentication"
            status=1
        fi
    else
        out=$("${cmd[@]}" <<< "$batch_content" 2>&1) || status=$?
    fi
    if [[ -n "$out" ]]; then
        echo "$out" | sed 's/^/[sftp] /' | tee -a "$LOG_FILE" >/dev/null
    fi
    return $status
}

# Test SFTP connection and ensure remote directory exists (mkdir -p like)
ensure_sftp_connection_and_remote_dir() {
    local hostkey_opts
    if ! hostkey_opts=$(prepare_host_key_options); then
        log_error "Host key verification setup failed"
        return 1
    fi

    # Basic connectivity test
    if ! run_sftp_batch "$hostkey_opts" $'pwd\nbye'; then
        log_error "Failed to connect to SFTP server ${SFTP_HOST}:${SFTP_PORT} as ${SFTP_USER}"
        # Cleanup any temp known_hosts
        if [[ -n "${SFTP_KNOWN_HOSTS_TEMP:-}" ]]; then rm -f "${SFTP_KNOWN_HOSTS_TEMP}" || true; fi
        return 1
    fi
    log "SFTP connectivity OK"

    # Check if remote dir exists; if not, create progressively
    if run_sftp_batch "$hostkey_opts" "ls ${SFTP_REMOTE_DIR}"$'\nbye'; then
        log "Remote directory exists: ${SFTP_REMOTE_DIR}"
        if [[ -n "${SFTP_KNOWN_HOSTS_TEMP:-}" ]]; then rm -f "${SFTP_KNOWN_HOSTS_TEMP}" || true; fi
        return 0
    fi

    log "Remote directory missing; creating: ${SFTP_REMOTE_DIR}"
    local path="${SFTP_REMOTE_DIR}"
    # Normalize path to absolute-like sequence processing
    # Build progressive paths and create when absent
    local IFS='/'
    local part
    local progressive=""
    # Preserve leading slash
    if [[ "$path" == /* ]]; then progressive="/"; fi
    # Read path components
    read -r -a parts <<< "${path#/}"
    local fallback_to_relative=false
    for part in "${parts[@]}"; do
        [[ -z "$part" ]] && continue
        if [[ "$progressive" == "/" ]]; then
            progressive="/${part}"
        elif [[ -z "$progressive" ]]; then
            progressive="$part"
        else
            progressive="${progressive}/${part}"
        fi
        # Check and create this level if needed
        if ! run_sftp_batch "$hostkey_opts" "ls ${progressive}"$'\nbye'; then
            if ! run_sftp_batch "$hostkey_opts" "mkdir ${progressive}"$'\nbye'; then
                # If absolute path creation fails (likely permission), try home-relative fallback once
                if [[ "$path" == /* ]]; then
                    fallback_to_relative=true
                    break
                fi
                log_error "Failed to create remote directory: ${progressive}"
                if [[ -n "${SFTP_KNOWN_HOSTS_TEMP:-}" ]]; then rm -f "${SFTP_KNOWN_HOSTS_TEMP}" || true; fi
                return 1
            fi
            log "Created remote directory: ${progressive}"
        fi
    done

    if [[ "$fallback_to_relative" == true ]]; then
        local rel
        if [[ "$path" == "/home/${SFTP_USER}/"* ]]; then
            rel="${path#/home/${SFTP_USER}/}"
        else
            rel="${path#/}"
        fi
        log "Falling back to home-relative path: ${rel}"
        progressive=""
        IFS='/' read -r -a parts <<< "$rel"
        for part in "${parts[@]}"; do
            [[ -z "$part" ]] && continue
            if [[ -z "$progressive" ]]; then
                progressive="$part"
            else
                progressive="${progressive}/${part}"
            fi
            if ! run_sftp_batch "$hostkey_opts" "ls ${progressive}"$'\nbye'; then
                if ! run_sftp_batch "$hostkey_opts" "mkdir ${progressive}"$'\nbye'; then
                    log_error "Failed to create remote directory (home-relative): ${progressive}"
                    if [[ -n "${SFTP_KNOWN_HOSTS_TEMP:-}" ]]; then rm -f "${SFTP_KNOWN_HOSTS_TEMP}" || true; fi
                    return 1
                fi
                log "Created remote directory: ${progressive}"
            fi
        done
        # Update SFTP_REMOTE_DIR to relative for subsequent upload
        SFTP_REMOTE_DIR="$rel"
    fi

    # Final confirmation
    if run_sftp_batch "$hostkey_opts" "ls ${SFTP_REMOTE_DIR}"$'\nbye'; then
        log "Remote directory ready: ${SFTP_REMOTE_DIR}"
        if [[ -n "${SFTP_KNOWN_HOSTS_TEMP:-}" ]]; then rm -f "${SFTP_KNOWN_HOSTS_TEMP}" || true; fi
        return 0
    else
        log_error "Remote directory validation failed: ${SFTP_REMOTE_DIR}"
        if [[ -n "${SFTP_KNOWN_HOSTS_TEMP:-}" ]]; then rm -f "${SFTP_KNOWN_HOSTS_TEMP}" || true; fi
        return 1
    fi
}

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

# Function to execute MySQL query and generate CSV locally
execute_query() {
    local sql_query="$1"
    local output_file="$2"
    
    # Remove any existing output file
    log "Ensuring no pre-existing output file: $output_file"
    rm -f "$output_file" 2>/dev/null || true
    
    log "Executing query to generate: $output_file"
    
    # Execute query and generate CSV locally using mysql client
    # Use tab-separated output and convert to CSV with proper quoting
    local mysql_status=1
    
    if [[ -n "$MYSQL_PASSWORD" ]]; then
        local tmp_defaults
        tmp_defaults=$(mktemp)
        printf "[client]\npassword=%s\n" "$MYSQL_PASSWORD" > "$tmp_defaults"
        
        # Execute query and generate CSV with headers
        if mysql --defaults-extra-file="$tmp_defaults" \
            -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" "${MYSQL_DATABASE}" \
            --batch --raw --skip-column-names \
            -e "SET NAMES utf8mb4; ${sql_query}" | \
            awk -v OFS="${CSV_FIELD_TERMINATOR}" -v QUOTE="${CSV_FIELD_ENCLOSURE}" '
            BEGIN {
                # Get headers first
                system("mysql --defaults-extra-file='"$tmp_defaults"' -h'"${MYSQL_HOST}"' -P'"${MYSQL_PORT}"' -u'"${MYSQL_USER}"' '"${MYSQL_DATABASE}"' --batch --skip-pager -e \"SET NAMES utf8mb4; ${sql_query}\" | head -1 | awk -v OFS=\"${CSV_FIELD_TERMINATOR}\" -v QUOTE=\"${CSV_FIELD_ENCLOSURE}\" '\''BEGIN{FS=\"\\t\"}{for(i=1;i<=NF;i++){gsub(QUOTE,QUOTE QUOTE,$i);printf \"%s%s%s\",QUOTE,$i,QUOTE; if(i<NF)printf OFS}print \"\"}'\''")
            }
            {
                FS="\t"
                for(i=1; i<=NF; i++) {
                    # Escape quotes by doubling them
                    gsub(QUOTE, QUOTE QUOTE, $i)
                    # Print field with quotes
                    printf "%s%s%s", QUOTE, $i, QUOTE
                    if(i < NF) printf OFS
                }
                print ""
            }' > "$output_file"; then
            mysql_status=0
        fi
        rm -f "$tmp_defaults"
    else
        # Execute query and generate CSV with headers
        if mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" "${MYSQL_DATABASE}" \
            --batch --raw --skip-column-names \
            -e "SET NAMES utf8mb4; ${sql_query}" | \
            awk -v OFS="${CSV_FIELD_TERMINATOR}" -v QUOTE="${CSV_FIELD_ENCLOSURE}" '
            BEGIN {
                # Get headers first
                system("mysql -h'"${MYSQL_HOST}"' -P'"${MYSQL_PORT}"' -u'"${MYSQL_USER}"' '"${MYSQL_DATABASE}"' --batch --skip-pager -e \"SET NAMES utf8mb4; ${sql_query}\" | head -1 | awk -v OFS=\"${CSV_FIELD_TERMINATOR}\" -v QUOTE=\"${CSV_FIELD_ENCLOSURE}\" '\''BEGIN{FS=\"\\t\"}{for(i=1;i<=NF;i++){gsub(QUOTE,QUOTE QUOTE,$i);printf \"%s%s%s\",QUOTE,$i,QUOTE; if(i<NF)printf OFS}print \"\"}'\''")
            }
            {
                FS="\t"
                for(i=1; i<=NF; i++) {
                    # Escape quotes by doubling them
                    gsub(QUOTE, QUOTE QUOTE, $i)
                    # Print field with quotes
                    printf "%s%s%s", QUOTE, $i, QUOTE
                    if(i < NF) printf OFS
                }
                print ""
            }' > "$output_file"; then
            mysql_status=0
        fi
    fi
    
    if [[ $mysql_status -ne 0 ]]; then
        log_error "Failed to execute query"
        return 1
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
    
    # Prepare host key checking options
    local hostkey_opts
    if ! hostkey_opts=$(prepare_host_key_options); then
        log_error "Host key verification setup failed"
        return 1
    fi

    # Create SFTP batch content (via stdin)
    local sftp_batch_content
    sftp_batch_content=$(cat <<EOF
cd $SFTP_REMOTE_DIR
put $local_file $remote_temp
rename $remote_temp $remote_filename
bye
EOF
)
    
    # Execute SFTP commands
    # Convert host key opts string to array
    local extra_opts=()
    if [[ -n "$hostkey_opts" ]]; then
        # shellcheck disable=SC2206
        extra_opts=($hostkey_opts)
    fi

    local cmd=( sftp )
    cmd+=( "${extra_opts[@]}" )
    if [[ -n "$SFTP_PASSWORD" && -z "$SFTP_KEY_FILE" ]]; then
        cmd+=( -o PreferredAuthentications=password -o PubkeyAuthentication=no -o BatchMode=no -o NumberOfPasswordPrompts=1 )
    fi
    cmd+=( -P "${SFTP_PORT}" -b - )
    if [[ -n "$SFTP_KEY_FILE" ]]; then
        cmd+=( -i "${SFTP_KEY_FILE}" )
    fi
    cmd+=( "${SFTP_USER}@${SFTP_HOST}" )

    if [[ "${DEBUG_SFTP:-false}" == "true" ]]; then
        {
            printf "[debug] sftp cmd: ";
            printf "%q " "${cmd[@]}";
            printf "\n";
        } | tee -a "$LOG_FILE" >/dev/null
    fi

    local out
    local status=0
    if [[ -n "$SFTP_PASSWORD" && -z "$SFTP_KEY_FILE" ]]; then
        if command -v sshpass >/dev/null 2>&1; then
            out=$(SSHPASS="$SFTP_PASSWORD" sshpass -e "${cmd[@]}" <<< "$sftp_batch_content" 2>&1) || status=$?
        else
            log_error "sshpass is required for password authentication but not installed"
            return 1
        fi
    else
        out=$("${cmd[@]}" <<< "$sftp_batch_content" 2>&1) || status=$?
    fi

    if [[ -n "$out" ]]; then
        echo "$out" | sed 's/^/[sftp] /' | tee -a "$LOG_FILE" >/dev/null
    fi

    if [[ $status -eq 0 ]]; then
        log "Successfully uploaded: $remote_final"
        if [[ -n "${SFTP_KNOWN_HOSTS_TEMP:-}" ]]; then rm -f "${SFTP_KNOWN_HOSTS_TEMP}" || true; fi
        unset SSHPASS
        return 0
    else
        log_error "Failed to upload file to SFTP"
        if [[ -n "${SFTP_KNOWN_HOSTS_TEMP:-}" ]]; then rm -f "${SFTP_KNOWN_HOSTS_TEMP}" || true; fi
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

    # File is generated locally, so no need for special copy handling
    local upload_source="$output_file"
    
    # Upload to SFTP
    if ! upload_to_sftp "$upload_source" "$csv_filename"; then
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
    
    # Ensure the OUTPUT_DIR exists for local CSV generation
    if [[ ! -d "${OUTPUT_DIR}" ]]; then
        log "Creating local output directory: ${OUTPUT_DIR}"
        mkdir -p "${OUTPUT_DIR}"
    fi

    # Ensure sshpass is available if password authentication is used for SFTP
    if ! ensure_sshpass_if_needed; then
        exit 1
    fi

    # Test SFTP connection and ensure remote directory exists (create if missing)
    if ! ensure_sftp_connection_and_remote_dir; then
        log_error "SFTP connection or remote directory setup failed"
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
