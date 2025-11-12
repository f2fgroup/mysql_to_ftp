#!/bin/bash
#
# Seed the database with test data using credentials from .env/config.env
# WARNING: This will ALTER your database for testing purposes (create tables and insert rows).
#
# - Creates tables if not exists inferred from example queries: users, categories, products, orders
# - Inserts at least 100 random rows per table (skips if already >= 100)
# - Idempotent-ish: re-running won't duplicate beyond the 100-row target per table
#
# Usage:
#   ./seed_database.sh [--config /path/to/config.env]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE=""

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
err() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2; }

usage() {
  cat <<EOF
Seed the configured MySQL database with test data.

Options:
  --config PATH   Path to env file (default: .env if exists, else config.env)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) CONFIG_FILE="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) err "Unknown argument: $1"; usage; exit 1;;
  esac
done

# Choose env file
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

MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3306}"

missing=0
for v in MYSQL_USER MYSQL_PASSWORD MYSQL_DATABASE; do
  if [[ -z "${!v:-}" ]]; then err "$v is required in ${CONFIG_FILE}"; missing=1; fi
done
if [[ ${missing} -ne 0 ]]; then exit 1; fi

echo ""
echo "=============================================================="
echo "WARNING: This script will CREATE TABLES and INSERT TEST DATA."
echo "Target DB: ${MYSQL_DATABASE} on ${MYSQL_HOST}:${MYSQL_PORT} as ${MYSQL_USER}"
echo "Type 'YES' to continue, anything else to abort."
echo "=============================================================="
read -r -p ">>> Confirm (YES to proceed): " CONFIRM
if [[ "${CONFIRM}" != "YES" ]]; then
  echo "Aborted by user."
  exit 1
fi

# Prepare a temporary defaults file for mysql client to avoid showing password
DEFAULTS_FILE="$(mktemp)"
cleanup() { rm -f "${DEFAULTS_FILE}" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cat >"${DEFAULTS_FILE}" <<EOF
[client]
password=${MYSQL_PASSWORD}
EOF

mysql_exec() {
  local sql="$1"
  mysql --defaults-extra-file="${DEFAULTS_FILE}" \
    -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" \
    "${MYSQL_DATABASE}" -e "${sql}"
}

mysql_exec_file() {
  local sql="$1"
  mysql --defaults-extra-file="${DEFAULTS_FILE}" \
    -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" \
    "${MYSQL_DATABASE}" <<< "${sql}"
}

log "Ensuring database exists..."
mysql --defaults-extra-file="${DEFAULTS_FILE}" -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -e "CREATE DATABASE IF NOT EXISTS \`${MYSQL_DATABASE}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"

log "Creating tables if not exists..."
mysql_exec_file "
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS categories (
  category_id INT AUTO_INCREMENT PRIMARY KEY,
  category_name VARCHAR(100) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS products (
  product_id INT AUTO_INCREMENT PRIMARY KEY,
  product_name VARCHAR(150) NOT NULL UNIQUE,
  category_id INT NOT NULL,
  price DECIMAL(10,2) NOT NULL DEFAULT 0,
  INDEX idx_products_category (category_id),
  CONSTRAINT fk_products_category FOREIGN KEY (category_id) REFERENCES categories(category_id)
    ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orders (
  order_id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  order_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  total_amount DECIMAL(10,2) NOT NULL DEFAULT 0,
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  INDEX idx_orders_customer (customer_id),
  CONSTRAINT fk_orders_user FOREIGN KEY (customer_id) REFERENCES users(id)
    ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"

# Helper: get row count for a table
get_count() {
  local tbl="$1"
  mysql --defaults-extra-file="${DEFAULTS_FILE}" -N -s \
    -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" \
    "${MYSQL_DATABASE}" -e "SELECT COUNT(*) FROM \`${tbl}\`;" 2>/dev/null || echo 0
}

# Insert up to target rows using a recursive CTE sequence
seed_users() {
  local target=100; local count; count=$(get_count "users" || echo 0)
  local need=$(( target - count )); if (( need <= 0 )); then log "users already has ${count} rows (>= ${target}), skipping"; return; fi
  log "Inserting ${need} user rows..."
  mysql_exec_file "
SET @i := 0;
INSERT INTO users (name, email, active, created_at)
SELECT CONCAT('User ', s.seq),
       CONCAT('user', s.seq, '@example.com'),
       IF(RAND() < 0.85, 1, 0),
       NOW() - INTERVAL FLOOR(RAND()*365) DAY
FROM (
  SELECT (@i := @i + 1 + ${count}) AS seq
  FROM (SELECT 1 FROM information_schema.COLUMNS LIMIT ${need}) t
) AS s
ON DUPLICATE KEY UPDATE name=VALUES(name), active=VALUES(active), created_at=VALUES(created_at);
"
}

seed_categories() {
  local target=100; local count; count=$(get_count "categories" || echo 0)
  local need=$(( target - count )); if (( need <= 0 )); then log "categories already has ${count} rows (>= ${target}), skipping"; return; fi
  log "Inserting ${need} category rows..."
  mysql_exec_file "
SET @i := 0;
INSERT INTO categories (category_name)
SELECT CONCAT('Category ', s.seq)
FROM (
  SELECT (@i := @i + 1 + ${count}) AS seq
  FROM (SELECT 1 FROM information_schema.COLUMNS LIMIT ${need}) t
) AS s
ON DUPLICATE KEY UPDATE category_name=VALUES(category_name);
"
}

seed_products() {
  local target=100; local count; count=$(get_count "products" || echo 0)
  local need=$(( target - count )); if (( need <= 0 )); then log "products already has ${count} rows (>= ${target}), skipping"; return; fi
  log "Inserting ${need} product rows..."
  mysql_exec_file "
SET @i := 0;
INSERT INTO products (product_name, category_id, price)
SELECT CONCAT('Product ', s.seq),
       (SELECT category_id FROM categories ORDER BY RAND() LIMIT 1),
       ROUND(RAND()*200 + 1, 2)
FROM (
  SELECT (@i := @i + 1 + ${count}) AS seq
  FROM (SELECT 1 FROM information_schema.COLUMNS LIMIT ${need}) t
) AS s
ON DUPLICATE KEY UPDATE product_name=VALUES(product_name), category_id=VALUES(category_id), price=VALUES(price);
"
}

seed_orders() {
  local target=100; local count; count=$(get_count "orders" || echo 0)
  local need=$(( target - count )); if (( need <= 0 )); then log "orders already has ${count} rows (>= ${target}), skipping"; return; fi
  log "Inserting ${need} order rows..."
  mysql_exec_file "
SET @i := 0;
INSERT INTO orders (customer_id, order_date, total_amount, status)
SELECT 
  (SELECT id FROM users ORDER BY RAND() LIMIT 1) AS customer_id,
  NOW() - INTERVAL FLOOR(RAND()*60) DAY AS order_date,
  ROUND(RAND()*500 + 10, 2) AS total_amount,
  ELT(FLOOR(RAND()*4)+1, 'pending','paid','shipped','cancelled') AS status
FROM (
  SELECT (@i := @i + 1) AS seq
  FROM (SELECT 1 FROM information_schema.COLUMNS LIMIT ${need}) t
) AS s;
"
}

seed_categories
seed_users
seed_products
seed_orders

log "Seeding complete. Row counts:"
for t in categories users products orders; do
  c=$(get_count "$t")
  echo " - $t: ${c} rows"
done

exit 0
