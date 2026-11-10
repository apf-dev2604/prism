#!/usr/bin/env bash
set -Eeuo pipefail
(( $# == 2 )) || { echo 'Usage: ensure_game_partitions.sh FROM TO' >&2; exit 2; }

FROM="$1"; TO="$2"
source "$HOME/scripts/prism/common/config/load_config.sh"
: "${DB_SETTINGS_PIPELINE_DB_HOST:?Missing DB_SETTINGS_PIPELINE_DB_HOST}"
: "${DB_SETTINGS_PIPELINE_DB_PORT:?Missing DB_SETTINGS_PIPELINE_DB_PORT}"
: "${DB_SETTINGS_PRD_RAW_DB_NAME:?Missing DB_SETTINGS_PRD_RAW_DB_NAME}"
: "${DB_SETTINGS_PARTITION_MAINT_USER:?Missing DB_SETTINGS_PARTITION_MAINT_USER from config.toml}"
: "${DBA_PARTITION_MAINT_PWD:?Missing DBA_PARTITION_MAINT_PWD from .env}"

if [[ "$DB_SETTINGS_PARTITION_MAINT_USER" != 'dba_partition_maint' ]]; then
  echo '[PARTITION ERROR] Unexpected partition maintenance username' >&2; exit 78
fi

# PG* variables are scoped to this one psql invocation; avoid command-line passwords.
PGHOST="$DB_SETTINGS_PIPELINE_DB_HOST" \
PGPORT="$DB_SETTINGS_PIPELINE_DB_PORT" \
PGDATABASE="$DB_SETTINGS_PRD_RAW_DB_NAME" \
PGUSER="$DB_SETTINGS_PARTITION_MAINT_USER" \
PGPASSWORD="$DBA_PARTITION_MAINT_PWD" \
PGOPTIONS='-c timezone=UTC -c lock_timeout=5000 -c statement_timeout=90000' \
psql -X -v ON_ERROR_STOP=1 -v from_date="$FROM" -v to_date="$TO" <<'SQL'
BEGIN;
SELECT public.ensure_gametx_partitions(:'from_date'::date, :'to_date'::date);
COMMIT;
SQL
printf '[PARTITION PREFLIGHT PASS] %s -> %s\n' "$FROM" "$TO"

