#!/usr/bin/env bash
# Insert keys in a range with random garbage values into kv_store table.
# Inserts only if key is not present using ON CONFLICT DO NOTHING.
# Usage: PG_CONNINFO='dbname=kvstore user=...' scripts/insert_random_kv.sh --start 1 --end 1000 --min-len 8 --max-len 512

set -euo pipefail
IFS=$'\n\t'

# Defaults
START=1
END=1000
MIN_LEN=8
MAX_LEN=256
BATCH_SIZE=100

print_help() {
  cat <<EOF
Usage: $0 [--start N] [--end N] [--min-len N] [--max-len N] [--batch N]
Environment:
  PG_CONNINFO    libpq connection string, e.g. "dbname=kvstore user=...". If not set, script will try to read config/db.json or default to "dbname=kvstore".

Example:
  PG_CONNINFO='dbname=kvstore user=pujith22' $0 --start 1 --end 1000 --min-len 16 --max-len 1024
EOF
}

# parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --start) START="$2"; shift 2;;
    --end) END="$2"; shift 2;;
    --min-len) MIN_LEN="$2"; shift 2;;
    --max-len) MAX_LEN="$2"; shift 2;;
    --batch) BATCH_SIZE="$2"; shift 2;;
    -h|--help) print_help; exit 0;;
    *) echo "Unknown arg: $1"; print_help; exit 1;;
  esac
done

# Determine psql connection
if [[ -n "${PG_CONNINFO-}" ]]; then
  PSQL_CONN="$PG_CONNINFO"
else
  # try config/db.json
  if [[ -f "config/db.json" ]]; then
    # extract conninfo field if present
    conninfo=$(jq -r '.conninfo // empty' config/db.json 2>/dev/null || true)
    if [[ -n "$conninfo" ]]; then
      PSQL_CONN="$conninfo"
    else
      PSQL_CONN="dbname=kvstore"
    fi
  else
    PSQL_CONN="dbname=kvstore"
  fi
fi

# check for psql
if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found in PATH. Install PostgreSQL client tools." >&2
  exit 2
fi

# helper: random string length L
random_string() {
  local len=$1
  # use base64 of urandom and cut to length; remove newlines
  head -c $(( (len*3/4) + 8 )) /dev/urandom | base64 | tr -d '\n' | cut -c1-$len
}

# Insert one key-value safely
insert_kv() {
  local key=$1
  local value=$2
  # use parameterized statement to avoid quoting issues
  psql "$PSQL_CONN" -qAt -c "INSERT INTO kv_store (key, value) VALUES ($1, \$VALUE\$${value}\$VALUE\$) ON CONFLICT (key) DO NOTHING;" >/dev/null
}

# Batch insertion: build SQL with multiple inserts for efficiency
batch_sql_start() {
  cat <<'SQL'
BEGIN;
SQL
}

batch_sql_end() {
  cat <<'SQL'
COMMIT;
SQL
}

# main loop
current=$START
count=0
while [[ $current -le $END ]]; do
  # compute random length between MIN_LEN and MAX_LEN
  # simplified arithmetic to avoid nested-paren parsing issues
  len=$(( MIN_LEN + RANDOM % (MAX_LEN - MIN_LEN + 1) ))
  val=$(random_string $len)
  # escape single quotes for safe SQL literal insertion
  value_escaped=$(printf "%s" "$val" | sed "s/'/''/g")
  # perform insert; ON CONFLICT DO NOTHING prevents overwriting existing keys
  psql "$PSQL_CONN" -v ON_ERROR_STOP=1 --quiet -c "INSERT INTO kv_store (key, value) VALUES (${current}, '${value_escaped}') ON CONFLICT (key) DO NOTHING;" >/dev/null
  count=$((count+1))
  if (( count % 100 == 0 )); then
    echo "Inserted/attempted $count keys (up to key $current)"
  fi
  current=$((current+1))
done

echo "Done. Processed $count keys."

exit 0
