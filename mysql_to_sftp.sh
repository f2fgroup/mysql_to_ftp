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
CSV_INCLUDE_HEADERS="${CSV_INCLUDE_HEADERS:-true}"

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

# Generate a CSV header line for a given SELECT query.
# Prints the header line (quoted CSV) to stdout and returns 0 on success.
# Returns 1 if headers cannot be derived.
generate_csv_header_line() {
    local sql_query="$1"

    # Heuristic: if the query already contains LIMIT, wrap as a derived table and LIMIT 0.
    # Otherwise, append LIMIT 0 at the end.
    local header_query=""
    if echo "$sql_query" | grep -qiE '(^|[[:space:]])limit[[:space:]]+[0-9]'; then
        header_query="SELECT * FROM ( ${sql_query} ) AS __t LIMIT 0"
    else
        header_query="${sql_query} LIMIT 0"
    fi

    local tsv_header
    local mysql_cmd=( mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" --batch --raw --column-names "${MYSQL_DATABASE}" )
    # Use a temporary defaults file if password is provided to avoid exposing it
    local tmp_defaults=""
    if [[ -n "${MYSQL_PASSWORD}" ]]; then
        tmp_defaults=$(mktemp)
        printf "[client]\npassword=%s\n" "${MYSQL_PASSWORD}" > "${tmp_defaults}"
        mysql_cmd=( mysql --defaults-extra-file="${tmp_defaults}" -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" --batch --raw --column-names "${MYSQL_DATABASE}" )
    fi

    # Execute the query and capture only the header line (first line)
    # Capture the first non-empty line of output as the header
    if ! tsv_header=$("${mysql_cmd[@]}" -e "SET NAMES utf8mb4; ${header_query}" 2>/dev/null | awk 'NF{print; exit}'); then
        [[ -n "${tmp_defaults}" ]] && rm -f "${tmp_defaults}"
        return 1
    fi
    [[ -n "${tmp_defaults}" ]] && rm -f "${tmp_defaults}"

    if [[ -n "${tsv_header}" ]]; then
        # Convert tab-separated header to quoted CSV, escaping double quotes by doubling them
        echo -n "${tsv_header}" | awk -v dq='"' 'BEGIN{FS="\t"} {
            for (i=1; i<=NF; i++) { gsub(dq, dq dq, $i) }
            for (i=1; i<=NF; i++) { printf "%s%s%s", dq, $i, dq; if (i<NF) printf "," }
            printf "\n"
        }'
        return 0
    fi

    # Fallback: derive headers from the SELECT list heuristically
    local select_part
    # Normalize whitespace to single spaces
    select_part=$(echo "$sql_query" | tr '\n' ' ' | sed -E 's/[[:space:]]+/ /g')
    # Extract text between SELECT and FROM (first FROM)
    select_part=$(echo "$select_part" | sed -E 's/^.*[Ss][Ee][Ll][Ee][Cc][Tt][[:space:]]+//; s/[[:space:]]+[Ff][Rr][Oo][Mm].*$//')
    if [[ -z "$select_part" ]]; then
        return 1
    fi
    # Split by commas (naive; suitable for simple select lists) and build CSV header
    echo "$select_part" | awk -v dq='"' '
        BEGIN{FS=","}
        {
            for (i=1; i<=NF; i++) {
                f=$i
                gsub(/^ +| +$/, "", f)
                # Lowercase copy for matching AS
                fl=f; gsub(/\`/,"",fl)
                # Extract alias after AS or last token
                header=f
                # Try AS alias
                if (match(fl, /[[:space:]]+[Aa][Ss][[:space:]]+([^ ]+)$/, m)) {
                    header = m[1]
                } else {
                    # Try last space separated token (alias without AS)
                    n=split(fl, parts, /[[:space:]]+/)
                    if (n>1) { header = parts[n] }
                }
                # If still contains dot, take the part after last dot
                gsub(/\`/ , "", header)
                if (match(header, /\./)) {
                    n=split(header, p, /\./); header=p[n]
                }
                # Strip double quotes/backticks
                gsub(/^"|"$/, "", header)
                gsub(/\`/, "", header)
                # CSV quote and escape
                gsub(/"/, "\"\"", header)
                printf "%s%s%s", dq, header, dq
                if (i<NF) printf ","
            }
            printf "\n"
        }
    '
    return 0
}

# Function to execute MySQL query with INTO OUTFILE
execute_query() {
    local sql_query="$1"
    local output_file="$2"
    
    # Proactively remove the output file (MySQL won't overwrite). Don't rely on -f, directory may not be listable.
    log "Ensuring no pre-existing output file: $output_file"
    rm -f "$output_file" 2>/dev/null || true
    if command -v sudo >/dev/null 2>&1; then
        sudo -n rm -f "$output_file" 2>/dev/null || true
    fi
    
    # Build the complete SQL statement
    local into_clause
    into_clause=$(build_into_outfile_clause "$output_file")
    
    local complete_query="${sql_query}
${into_clause};"
    
    log "Executing query to generate: $output_file"
    
    # Execute with optional defaults file to avoid exposing password
    local full_query="SET NAMES utf8mb4; ${complete_query}"
    local mysql_status=1
    if [[ -n "$MYSQL_PASSWORD" ]]; then
        local tmp_defaults
        tmp_defaults=$(mktemp)
        printf "[client]\npassword=%s\n" "$MYSQL_PASSWORD" > "$tmp_defaults"
        if mysql --defaults-extra-file="$tmp_defaults" -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" "${MYSQL_DATABASE}" -e "$full_query"; then
            mysql_status=0
        fi
        rm -f "$tmp_defaults"
    else
        if mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" "${MYSQL_DATABASE}" -e "$full_query"; then
            mysql_status=0
        fi
    fi
    if [[ $mysql_status -ne 0 ]]; then
        log_error "Failed to execute query"
        return 1
    fi

    # Verify the output file was created (best-effort): try direct test, then sudo test; otherwise trust MySQL success
    if [[ -f "$output_file" ]]; then
        : # ok
    elif command -v sudo >/dev/null 2>&1 && sudo -n test -f "$output_file" 2>/dev/null; then
        : # ok (verified with sudo)
    else
        log_error "Output file was not created: $output_file"
        # Provide diagnostics to help troubleshoot permissions and secure_file_priv
        local current_secure
        current_secure=$(get_secure_file_priv || true)
        log "Diagnostic - secure_file_priv: '${current_secure:-unknown}'"
        if command -v ls >/dev/null 2>&1; then
            if [[ -d "$(dirname "$output_file")" ]]; then
                log "Diagnostic - directory listing of $(dirname "$output_file"):" 
                ls -ld "$(dirname "$output_file")" | tee -a "$LOG_FILE" || true
                ls -l "$(dirname "$output_file")" | head -n 20 | tee -a "$LOG_FILE" || true
            fi
        fi
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

    # Pre-clean any existing output to avoid MySQL 'file exists' error; use sudo if required and do not rely on -f
    log "Pre-clean: removing any existing output file: $output_file"
    rm -f "$output_file" 2>/dev/null || true
    if command -v sudo >/dev/null 2>&1; then
        sudo -n rm -f "$output_file" 2>/dev/null || true
    fi
    # If still exists (cannot stat reliably without permissions, so attempt a safe rename target to avoid collision)
    if [[ -e "$output_file" ]]; then
        local ts
        ts="$(date +%Y%m%d%H%M%S)"
        local uniq_output_file="${OUTPUT_DIR}/${basename}_${ts}_$RANDOM.csv"
        log "Using unique output file to avoid potential collision: $uniq_output_file"
        output_file="$uniq_output_file"
    fi
    
    # Execute query to generate CSV
    if ! execute_query "$query" "$output_file"; then
        log_error "Failed to execute query for: $sql_file"
        return 1
    fi

    # Prepare a readable data copy if needed
    local data_source="$output_file"
    if [[ ! -r "$output_file" ]]; then
        local tmp_data_copy
        tmp_data_copy="/tmp/${basename}_data.csv"
        if command -v sudo >/dev/null 2>&1; then
            if sudo -n cp "$output_file" "$tmp_data_copy" 2>/dev/null; then
                sudo -n chown "$(id -u)":"$(id -g)" "$tmp_data_copy" 2>/dev/null || true
                sudo -n chmod 640 "$tmp_data_copy" 2>/dev/null || true
                data_source="$tmp_data_copy"
                log "Created readable data copy: $tmp_data_copy"
            else
                log_error "Could not create readable copy of $output_file"
                return 1
            fi
        else
            log_error "Output file is not readable and sudo is not available: $output_file"
            return 1
        fi
    fi

    # Optionally prepend CSV header row
    local upload_source="$data_source"
    if [[ "${CSV_INCLUDE_HEADERS}" == "true" ]]; then
        log "Generating CSV header row for: ${csv_filename}"
        local header_line
        if header_line=$(generate_csv_header_line "$query"); then
            local tmp_with_header="/tmp/${basename}_with_header.csv"
            printf "%s\n" "$header_line" > "$tmp_with_header"
            cat "$data_source" >> "$tmp_with_header"
            upload_source="$tmp_with_header"
            log "Header row added to temporary file: $tmp_with_header"
        else
            log "Could not derive header row; proceeding without headers for: ${csv_filename}"
        fi
    fi
    
    # Upload to SFTP
    if ! upload_to_sftp "$upload_source" "$csv_filename"; then
        log_error "Failed to upload CSV for: $sql_file"
        return 1
    fi

    # Cleanup temporary files created for upload
    if [[ "$upload_source" == /tmp/* ]]; then
        rm -f "$upload_source" 2>/dev/null || true
        log "Removed temporary upload file: $upload_source"
    fi
    if [[ "$data_source" == /tmp/* && "$data_source" != "$upload_source" ]]; then
        rm -f "$data_source" 2>/dev/null || true
        log "Removed temporary data file: $data_source"
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
    
    # Align OUTPUT_DIR with MySQL secure_file_priv if set
    local secure_dir
    secure_dir=$(get_secure_file_priv || true)
    if [[ -n "${secure_dir}" ]]; then
        if [[ "${OUTPUT_DIR}" != "${secure_dir}" ]]; then
            log "secure_file_priv is '${secure_dir}', overriding OUTPUT_DIR='${OUTPUT_DIR}' to match"
            OUTPUT_DIR="${secure_dir}"
        fi
    else
        log "secure_file_priv not reported or unrestricted; using OUTPUT_DIR='${OUTPUT_DIR}'"
    fi

    # Ensure the OUTPUT_DIR exists and is writable by MySQL (may require sudo)
    ensure_output_dir_permissions "${OUTPUT_DIR}"

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
