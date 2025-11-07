# Usage: source scripts/setup_pg_env.zsh
# Sets PG_CONNINFO environment variable for this shell session.

# Edit the values below as needed for your local Postgres.
: "${PG_DBNAME:=kvstore}"
: "${PG_USER:=$USER}"
: "${PG_HOST:=127.0.0.1}"
: "${PG_PORT:=5432}"
# Leave password empty to use .pgpass or trust/local auth. Set if required.
: "${PG_PASSWORD:=}"

if [ -n "$PG_PASSWORD" ]; then
  export PG_CONNINFO="dbname=${PG_DBNAME} user=${PG_USER} host=${PG_HOST} port=${PG_PORT} password=${PG_PASSWORD}"
else
  export PG_CONNINFO="dbname=${PG_DBNAME} user=${PG_USER} host=${PG_HOST} port=${PG_PORT}"
fi

echo "PG_CONNINFO set to: $PG_CONNINFO"
echo "Tip: To persist in this shell, always source this file: source scripts/setup_pg_env.zsh"
