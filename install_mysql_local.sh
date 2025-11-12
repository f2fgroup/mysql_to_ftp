#!/bin/bash
#
# Install and configure a local MySQL Server using values from config.env/.env
# - Installs mysql-server and mysql-client via apt
# - Ensures secure_file_priv matches OUTPUT_DIR
# - Creates MYSQL_DATABASE and MYSQL_USER with MYSQL_PASSWORD
# - Grants required privileges including FILE for INTO OUTFILE
# - Idempotent: safe to run multiple times
#
# Usage:
#   ./install_mysql_local.sh [--config /path/to/config.env]
#
# Requirements:
# - Ubuntu/Debian with apt
# - Root privileges (or sudo access)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE=""

# --- logging helpers ---
log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2; }

# --- small utils ---
need_sudo() { [ "$(id -u)" -ne 0 ]; }

run_root() {
  if need_sudo; then sudo -n bash -c "$1"; else bash -c "$1"; fi
}

apt_install() {
  local pkgs=("$@")
  if need_sudo; then
    sudo -n env DEBIAN_FRONTEND=noninteractive apt-get update -y
    sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -yq "${pkgs[@]}"
  else
    DEBIAN_FRONTEND=noninteractive apt-get update -y
    DEBIAN_FRONTEND=noninteractive apt-get install -yq "${pkgs[@]}"
  fi
}

start_mysql_service() {
  # Prefer systemctl only when systemd is actually running (not the case in many containers)
  if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ] && [ "$(ps -p 1 -o comm= 2>/dev/null | tr -d ' ')" = "systemd" ]; then
    if need_sudo; then
      sudo -n systemctl enable --now mysql || sudo -n systemctl restart mysql
    else
      systemctl enable --now mysql || systemctl restart mysql
    fi
  else
    # Fallback to SysV-style service management (works well in Docker)
    if need_sudo; then
      sudo -n service mysql start || sudo -n service mysql restart || sudo -n /etc/init.d/mysql start || true
    else
      service mysql start || service mysql restart || /etc/init.d/mysql start || true
    fi
  fi
}

# Wait for MySQL to be ready (TCP ping)
wait_for_mysql() {
  local retries=30
  local i
  for i in $(seq 1 ${retries}); do
    if mysqladmin --protocol=TCP -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" ping --silent >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Escape single quotes for SQL string literal
sql_quote() {
  local s="$1"
  # Double single quotes for SQL string literal escaping
  printf "%s" "$(printf "%s" "$s" | sed "s/'/''/g")"
}

usage() {
  cat <<EOF
Install and configure local MySQL using config.env/.env

Options:
  --config PATH   Path to env file (default: .env if exists, else config.env)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_FILE="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      err "Unknown argument: $1"; usage; exit 1;;
  esac
done

# Locate env file
if [[ -z "${CONFIG_FILE}" ]]; then
  if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    CONFIG_FILE="${SCRIPT_DIR}/.env"
  else
    CONFIG_FILE="${SCRIPT_DIR}/config.env"
  fi
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
  err "Config file not found: ${CONFIG_FILE}"
  exit 1
fi

log "Using config file: ${CONFIG_FILE}"
# shellcheck disable=SC1090
set -a; source "${CONFIG_FILE}"; set +a

# Defaults if missing in env
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
OUTPUT_DIR="${OUTPUT_DIR:-/var/lib/mysql-files}"

# Validate required values
missing=0
for v in MYSQL_USER MYSQL_PASSWORD MYSQL_DATABASE; do
  if [[ -z "${!v:-}" ]]; then err "$v is required in ${CONFIG_FILE}"; missing=1; fi
done
if [[ ${missing} -ne 0 ]]; then exit 1; fi

log "Installing MySQL server and client..."
apt_install mysql-server mysql-client

log "Ensuring MySQL service is running..."
start_mysql_service

# Ensure socket directory exists in containerized environments
run_root "mkdir -p /run/mysqld && chown mysql:mysql /run/mysqld && chmod 755 /run/mysqld" || true

log "Waiting for MySQL to be ready..."
if ! wait_for_mysql; then
  err "MySQL did not become ready in time on ${MYSQL_HOST}:${MYSQL_PORT}"
  exit 1
fi

# Ensure output directory exists and is owned by mysql
log "Configuring secure file directory: ${OUTPUT_DIR}"
run_root "mkdir -p '${OUTPUT_DIR}'"
run_root "chown mysql:mysql '${OUTPUT_DIR}'"
run_root "chmod 750 '${OUTPUT_DIR}'"

# Configure secure_file_priv via drop-in file
LOCAL_CNF="/etc/mysql/mysql.conf.d/99-secure-file-priv.cnf"
TMP_CNF="$(mktemp)"
cat >"${TMP_CNF}" <<EOF
[mysqld]
secure-file-priv = ${OUTPUT_DIR}
EOF

log "Writing MySQL config: ${LOCAL_CNF}"
if need_sudo; then
  sudo -n tee "${LOCAL_CNF}" >/dev/null <"${TMP_CNF}"
else
  cat "${TMP_CNF}" > "${LOCAL_CNF}"
fi
rm -f "${TMP_CNF}"

log "Restarting MySQL to apply configuration..."
start_mysql_service

# Build SQL statements
_db="$(sql_quote "${MYSQL_DATABASE}")"
_user="$(sql_quote "${MYSQL_USER}")"
_pass="$(sql_quote "${MYSQL_PASSWORD}")"

SQL_SETUP=$(cat <<EOSQL
CREATE DATABASE IF NOT EXISTS \`${_db}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE USER IF NOT EXISTS '${_user}'@'localhost' IDENTIFIED BY '${_pass}';
GRANT ALL PRIVILEGES ON \`${_db}\`.* TO '${_user}'@'localhost';
GRANT FILE ON *.* TO '${_user}'@'localhost';
FLUSH PRIVILEGES;
-- show secure_file_priv to confirm
SHOW VARIABLES LIKE 'secure_file_priv';
EOSQL
)

log "Applying database and user configuration..."

# On Ubuntu, root often authenticates via unix_socket. Run mysql as root user.
if need_sudo; then
  if ! echo "${SQL_SETUP}" | sudo -n mysql -uroot; then
    err "Failed to apply MySQL configuration as root"; exit 1; fi
else
  if ! echo "${SQL_SETUP}" | mysql -uroot; then
    err "Failed to apply MySQL configuration as root"; exit 1; fi
fi

# Quick connectivity check with the new user
log "Verifying connection as ${MYSQL_USER} to ${MYSQL_DATABASE}..."
if ! mysql --protocol=TCP -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DATABASE}" -e "SELECT 1 AS ok;" >/dev/null 2>&1; then
  err "Login test failed for ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}"
  exit 1
fi

# Show secure_file_priv for confirmation using app user over TCP (avoid socket)
mysql --protocol=TCP -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DATABASE}" \
  -e "SHOW VARIABLES LIKE 'secure_file_priv';" || true

log "MySQL installation and configuration complete."
log "- Database: ${MYSQL_DATABASE}"
log "- User: ${MYSQL_USER} (granted FILE and full access to DB)"
log "- secure_file_priv: ${OUTPUT_DIR}"
log "You can now run ./mysql_to_sftp.sh after ensuring SFTP settings are configured."

exit 0
