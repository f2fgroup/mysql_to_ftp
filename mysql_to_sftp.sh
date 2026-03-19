#!/bin/bash
#
# MySQL to SFTP Extraction Script
# Executes SQL queries from .sql files, generates CSV files using the MySQL
# client's batch mode, and uploads them to an SFTP server using atomic operations.
#
# Requirements:
# - bash, mysql client, sftp
# - MySQL user with SELECT privilege
#

set -euo pipefail

# --- Config loading: support --config or auto-load .env/config.env ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE=""

# Parse --config using index-based iteration to avoid consuming $@
_i=1
while [[ $_i -le $# ]]; do
    _arg="${!_i}"
    case "$_arg" in
        --config=*)
            CONFIG_FILE="${_arg#*=}"
            ;;
        --config)
            _j=$(( _i + 1 ))
            if [[ $_j -gt $# ]] || [[ -z "${!_j:-}" ]]; then
                echo "Error: --config requires a non-empty value" >&2
                exit 1
            fi
            CONFIG_FILE="${!_j}"
            ;;
    esac
    _i=$(( _i + 1 ))
done
unset _i _j _arg

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
LOG_DIR="${LOG_DIR:-/tmp/mysql_to_sftp_logs}"
# If LOG_FILE is not explicitly set, generate a timestamped log file per execution
if [[ -z "${LOG_FILE:-}" ]]; then
    _LOG_RUN_TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
    LOG_FILE="${LOG_DIR}/mysql_to_sftp_${_LOG_RUN_TIMESTAMP}.log"
fi
# Ensure the log directory exists
mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null || true
SFTP_DISABLE_HOST_KEY_CHECKING="${SFTP_DISABLE_HOST_KEY_CHECKING:-false}"
SFTP_KNOWN_HOSTS="${SFTP_KNOWN_HOSTS:-}"
# Default to true even when the variable is set but empty
SFTP_UPLOAD_LOG="${SFTP_UPLOAD_LOG:-true}"
[[ -z "${SFTP_UPLOAD_LOG}" ]] && SFTP_UPLOAD_LOG="true"
# Default to upload/logs when the variable is unset or empty
SFTP_LOG_REMOTE_DIR="${SFTP_LOG_REMOTE_DIR:-upload/logs}"
[[ -z "${SFTP_LOG_REMOTE_DIR}" ]] && SFTP_LOG_REMOTE_DIR="upload/logs"

# CSV format settings
CSV_FIELD_TERMINATOR=','
CSV_FIELD_ENCLOSURE='"'

# Global temp file paths registered for cleanup on exit
SFTP_KNOWN_HOSTS_TEMP=""
_TMP_DEFAULTS=""

_cleanup() {
    rm -f "${SFTP_KNOWN_HOSTS_TEMP:-}" "${_TMP_DEFAULTS:-}"
}
trap _cleanup EXIT

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# Function to log error messages
log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$LOG_FILE" >&2
}

# Function to write debug messages to log file only (not terminal)
log_debug() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"
}

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

# Prepare host key verification options for sftp (call once; result is reused for all connections)
prepare_host_key_options() {
    local opts=""
    SFTP_KNOWN_HOSTS_TEMP=""
    if [[ "${SFTP_DISABLE_HOST_KEY_CHECKING}" == "true" ]]; then
        log_debug "Host key verification is DISABLED (StrictHostKeyChecking=no)"
        opts+=" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o GlobalKnownHostsFile=/dev/null"
        echo "$opts"
        return 0
    fi

    # Strict host key checking with provided known_hosts
    if [[ -n "${SFTP_KNOWN_HOSTS}" && -f "${SFTP_KNOWN_HOSTS}" ]]; then
        log_debug "Using provided known_hosts file: ${SFTP_KNOWN_HOSTS}"
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
    ssh-keyscan -p "${SFTP_PORT}" -T 5 "${SFTP_HOST}" >"${SFTP_KNOWN_HOSTS_TEMP}" 2>/dev/null || true
    if [[ ! -s "${SFTP_KNOWN_HOSTS_TEMP}" ]]; then
        log_error "Failed to retrieve host key from ${SFTP_HOST}:${SFTP_PORT} (empty response)."
        rm -f "${SFTP_KNOWN_HOSTS_TEMP}"
        SFTP_KNOWN_HOSTS_TEMP=""
        return 1
    fi
    log_debug "Using generated known_hosts for ${SFTP_HOST}:${SFTP_PORT}"
    opts+=" -o StrictHostKeyChecking=yes -o UserKnownHostsFile=${SFTP_KNOWN_HOSTS_TEMP}"
    echo "$opts"
    return 0
}

# Run an sftp batch with prepared options; expects hostkey_opts in "$1" and batch content in "$2"
run_sftp_batch() {
    local hostkey_opts="$1"
    local batch_content="$2"

    # Reset IFS to default to prevent contamination from callers that modified it
    local IFS=$' \t\n'

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
            printf "[debug] sftp cmd: "
            printf "%q " "${cmd[@]}"
            printf "\n"
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
# Takes pre-computed hostkey_opts as $1
ensure_sftp_connection_and_remote_dir() {
    local hostkey_opts="$1"

    # Basic connectivity test
    if ! run_sftp_batch "$hostkey_opts" $'pwd\nbye'; then
        log_error "Failed to connect to SFTP server ${SFTP_HOST}:${SFTP_PORT} as ${SFTP_USER}"
        return 1
    fi
    log "SFTP connectivity OK"

    # Check if remote dir exists; if not, create progressively
    if run_sftp_batch "$hostkey_opts" "$(printf 'ls "%s"\nbye' "$SFTP_REMOTE_DIR")"; then
        log "Remote directory exists: ${SFTP_REMOTE_DIR}"
        return 0
    fi

    log "Remote directory missing; creating: ${SFTP_REMOTE_DIR}"
    local path="${SFTP_REMOTE_DIR}"
    local part
    local progressive=""
    local parts=()
    # Preserve leading slash
    if [[ "$path" == /* ]]; then progressive="/"; fi
    # IFS='/' scoped to the read command only — does not modify the global IFS
    IFS='/' read -r -a parts <<< "${path#/}"
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
        if ! run_sftp_batch "$hostkey_opts" "$(printf 'ls "%s"\nbye' "$progressive")"; then
            if ! run_sftp_batch "$hostkey_opts" "$(printf 'mkdir "%s"\nbye' "$progressive")"; then
                # If absolute path creation fails (likely permission), try home-relative fallback once
                if [[ "$path" == /* ]]; then
                    fallback_to_relative=true
                    break
                fi
                log_error "Failed to create remote directory: ${progressive}"
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
            if ! run_sftp_batch "$hostkey_opts" "$(printf 'ls "%s"\nbye' "$progressive")"; then
                if ! run_sftp_batch "$hostkey_opts" "$(printf 'mkdir "%s"\nbye' "$progressive")"; then
                    log_error "Failed to create remote directory (home-relative): ${progressive}"
                    return 1
                fi
                log "Created remote directory: ${progressive}"
            fi
        done
        # Side-effect: update SFTP_REMOTE_DIR to home-relative path for subsequent uploads
        SFTP_REMOTE_DIR="$rel"
    fi

    # Final confirmation
    if run_sftp_batch "$hostkey_opts" "$(printf 'ls "%s"\nbye' "$SFTP_REMOTE_DIR")"; then
        log "Remote directory ready: ${SFTP_REMOTE_DIR}"
        return 0
    else
        log_error "Remote directory validation failed: ${SFTP_REMOTE_DIR}"
        return 1
    fi
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

    if [[ -n "$SFTP_KEY_FILE" && ! -f "$SFTP_KEY_FILE" ]]; then
        log_error "SFTP_KEY_FILE does not exist or is not readable: $SFTP_KEY_FILE"
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
    sed 's/[[:space:]]*;[[:space:]]*$//' "$sql_file"
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

# AWK script: decode MySQL --batch escape sequences and format fields as CSV.
# Uses a single-pass unescape() function to correctly handle all MySQL escapes
# including \\ without the need for a sentinel character.
_AWK_CSV='
    function unescape(s,    r, i, c, nc) {
        r = ""; i = 1
        while (i <= length(s)) {
            c = substr(s, i, 1)
            if (c == "\\") {
                nc = substr(s, i+1, 1)
                if      (nc == "n")  { r = r "\n"; i += 2 }
                else if (nc == "r")  { r = r "\r"; i += 2 }
                else if (nc == "t")  { r = r "\t"; i += 2 }
                else if (nc == "\\") { r = r "\\"; i += 2 }
                else if (nc == "N")  { r = r "";   i += 2 }
                else if (nc == "0")  { r = r "\0"; i += 2 }
                else                 { r = r c;    i++    }
            } else {
                r = r c; i++
            }
        }
        return r
    }
    BEGIN { FS="\t" }
    {
        for(i=1; i<=NF; i++) {
            val = unescape($i)
            gsub(QUOTE, QUOTE QUOTE, val)
            printf "%s%s%s", QUOTE, val, QUOTE
            if(i < NF) printf OFS
        }
        print ""
    }
'

# Function to execute MySQL query and generate CSV locally
execute_query() {
    local sql_query="$1"
    local output_file="$2"

    log "Ensuring no pre-existing output file: $output_file"
    rm -f "$output_file" 2>/dev/null || true

    log "Executing query to generate: $output_file"
    log "CSV headers enabled (always included)"

    local mysql_status=1

    if [[ -n "$MYSQL_PASSWORD" ]]; then
        local tmp_defaults
        tmp_defaults=$(mktemp)
        _TMP_DEFAULTS="$tmp_defaults"  # register for EXIT trap cleanup
        printf "[client]\npassword=%s\n" "$MYSQL_PASSWORD" > "$tmp_defaults"

        if mysql --defaults-extra-file="$tmp_defaults" \
            -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" "${MYSQL_DATABASE}" \
            --default-character-set=utf8mb4 \
            --batch \
            -e "${sql_query}" | \
            awk -v OFS="${CSV_FIELD_TERMINATOR}" -v QUOTE="${CSV_FIELD_ENCLOSURE}" \
                "$_AWK_CSV" > "$output_file"; then
            mysql_status=0
        fi

        rm -f "$tmp_defaults"
        _TMP_DEFAULTS=""
    else
        if mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" "${MYSQL_DATABASE}" \
            --default-character-set=utf8mb4 \
            --batch \
            -e "${sql_query}" | \
            awk -v OFS="${CSV_FIELD_TERMINATOR}" -v QUOTE="${CSV_FIELD_ENCLOSURE}" \
                "$_AWK_CSV" > "$output_file"; then
            mysql_status=0
        fi
    fi

    if [[ $mysql_status -ne 0 ]]; then
        rm -f "$output_file"
        log_error "Failed to execute query"
        return 1
    fi

    if [[ ! -f "$output_file" ]]; then
        log_error "Output file was not created: $output_file"
        return 1
    fi

    if [[ ! -s "$output_file" ]]; then
        rm -f "$output_file"
        log_error "Output file is empty (query produced no output): $output_file"
        return 1
    fi

    log "Successfully generated CSV file: $output_file"
    return 0
}

# Function to upload file to SFTP using atomic operation (put to .part, then rename)
upload_to_sftp() {
    local local_file="$1"
    local remote_filename="$2"
    local hostkey_opts="$3"

    local remote_temp="${remote_filename}.part"
    local remote_final="${SFTP_REMOTE_DIR}/${remote_filename}"

    log "Uploading file to SFTP: $local_file -> $remote_final"

    local sftp_batch_content
    sftp_batch_content=$(printf 'cd "%s"\nput "%s" "%s"\nrename "%s" "%s"\nbye\n' \
        "$SFTP_REMOTE_DIR" "$local_file" "$remote_temp" "$remote_temp" "$remote_filename")

    if run_sftp_batch "$hostkey_opts" "$sftp_batch_content"; then
        log "Successfully uploaded: $remote_final"
        return 0
    else
        log_error "Failed to upload file to SFTP"
        return 1
    fi
}

# Upload the execution log file to the SFTP server (non-fatal on failure)
upload_log_to_sftp() {
    local hostkey_opts="$1"

    [[ "${SFTP_UPLOAD_LOG}" != "true" ]] && return 0
    [[ ! -f "${LOG_FILE}" ]] && return 0

    local log_filename
    log_filename="$(basename "${LOG_FILE}")"
    local remote_temp="${log_filename}.part"

    log "Uploading log file to SFTP: ${SFTP_LOG_REMOTE_DIR}/${log_filename}"

    # Attempt to create the remote log directory (not fatal if it already exists)
    run_sftp_batch "$hostkey_opts" "$(printf 'mkdir "%s"\nbye' "${SFTP_LOG_REMOTE_DIR}")" >/dev/null 2>&1 || true

    local sftp_batch
    sftp_batch="$(printf 'cd "%s"\nput "%s" "%s"\nrename "%s" "%s"\nbye\n' \
        "${SFTP_LOG_REMOTE_DIR}" "${LOG_FILE}" "${remote_temp}" "${remote_temp}" "${log_filename}")"

    if run_sftp_batch "$hostkey_opts" "$sftp_batch"; then
        log "Log file uploaded successfully: ${SFTP_LOG_REMOTE_DIR}/${log_filename}"
    else
        log_error "Failed to upload log file to SFTP (non-fatal)"
    fi
}

# Function to process a single SQL file
process_sql_file() {
    local sql_file="$1"
    local hostkey_opts="$2"
    local basename
    basename=$(basename "$sql_file" .sql)
    local csv_filename="${basename}.csv"
    local output_file="${OUTPUT_DIR}/${csv_filename}"

    log "Processing SQL file: $sql_file"

    local query
    query=$(clean_sql_query "$sql_file")

    if [[ -z "$query" ]]; then
        log_error "Empty query in file: $sql_file"
        return 1
    fi

    if check_into_outfile "$query"; then
        log_error "Query already contains INTO OUTFILE clause: $sql_file"
        return 1
    fi

    if ! execute_query "$query" "$output_file"; then
        log_error "Failed to execute query for: $sql_file"
        return 1
    fi

    if ! upload_to_sftp "$output_file" "$csv_filename" "$hostkey_opts"; then
        log_error "Failed to upload CSV for: $sql_file"
        return 1
    fi

    # Uncomment to remove local CSV files after upload:
    # rm -f "$output_file"

    log "Successfully processed: $sql_file"
    return 0
}

# Main execution
main() {
    log "=== MySQL to SFTP Extraction Script Started ==="

    if ! validate_config; then
        log_error "Configuration validation failed"
        exit 1
    fi

    if [[ ! -d "${OUTPUT_DIR}" ]]; then
        log "Creating local output directory: ${OUTPUT_DIR}"
        mkdir -p "${OUTPUT_DIR}"
    fi

    if ! ensure_sshpass_if_needed; then
        exit 1
    fi

    # Prepare host key options once — reused for all SFTP operations
    local hostkey_opts
    if ! hostkey_opts=$(prepare_host_key_options); then
        log_error "Host key verification setup failed"
        exit 1
    fi

    if ! ensure_sftp_connection_and_remote_dir "$hostkey_opts"; then
        log_error "SFTP connection or remote directory setup failed"
        exit 1
    fi

    log "Configuration validated successfully"
    log "SQL directory: $SQL_DIR"
    log "Output directory: $OUTPUT_DIR"
    log "MySQL database: $MYSQL_DATABASE"
    log "SFTP host: $SFTP_HOST"

    # Collect .sql files robustly (handles filenames with spaces or special characters)
    local sql_files=()
    while IFS= read -r -d '' sql_file; do
        sql_files+=("$sql_file")
    done < <(find "$SQL_DIR" -type f -name "*.sql" -print0 | sort -z)

    if [[ ${#sql_files[@]} -eq 0 ]]; then
        log "No SQL files found in: $SQL_DIR"
        exit 0
    fi

    local total_files=${#sql_files[@]}
    log "Found $total_files SQL file(s) to process"

    local success_count=0
    local error_count=0

    for sql_file in "${sql_files[@]}"; do
        if process_sql_file "$sql_file" "$hostkey_opts"; then
            success_count=$((success_count + 1))
        else
            error_count=$((error_count + 1))
        fi
    done

    log "=== MySQL to SFTP Extraction Script Completed ==="
    log "Total files: $total_files"
    log "Successful: $success_count"
    log "Errors: $error_count"
    log "Log file: ${LOG_FILE}"

    upload_log_to_sftp "$hostkey_opts"

    if [[ $error_count -gt 0 ]]; then
        exit 1
    fi

    exit 0
}

# Run main function
main "$@"
