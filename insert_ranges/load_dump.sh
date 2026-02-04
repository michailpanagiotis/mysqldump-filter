#!/bin/bash
set -euo pipefail

MYSQL_CONFIG="$HOME/.config/mysql/local.cnf"

DUMP_FILE="${1:?Usage: $0 <dump.sql> [tables to exclude...]}"
shift
EXCLUDE_TABLES="${*:-}"

# Build exclude flags
EXCLUDE_FLAGS=""
for table in $EXCLUDE_TABLES; do
    EXCLUDE_FLAGS="$EXCLUDE_FLAGS -x $table"
done

# Extract database name from config
DATABASE=$(grep -E '^database\s*=' "$MYSQL_CONFIG" | sed 's/^database\s*=\s*//' | tr -d '[:space:]')
if [[ -z "$DATABASE" ]]; then
    echo "Error: No database specified in $MYSQL_CONFIG" >&2
    exit 1
fi

# Verify MySQL is accessible
if ! mysql --defaults-extra-file="$MYSQL_CONFIG" --database="" -e "SELECT 1" >/dev/null; then
    exit 1
fi

# Drop and create database
mysql --defaults-extra-file="$MYSQL_CONFIG" --database="" -e "DROP DATABASE IF EXISTS \`$DATABASE\`; CREATE DATABASE \`$DATABASE\`"

# Load into MySQL
# insert_ranges "$DUMP_FILE" $EXCLUDE_FLAGS -o - | mysql --defaults-extra-file="$MYSQL_CONFIG"
