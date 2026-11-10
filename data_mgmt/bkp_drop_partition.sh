#!/usr/bin/env bash
# DISABLED BY DEFAULT. Back up before any partition removal; keep newest seven.
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$HOME/scripts/prism/common/config/load_config.sh"
: "${DB_SETTINGS_PIPELINE_DB_HOST:?}" "${DB_SETTINGS_PIPELINE_DB_PORT:?}"
: "${DB_SETTINGS_PRD_RAW_DB_NAME:?}" "${DB_SETTINGS_PARTITION_MAINT_USER:?}" "${DBA_PARTITION_MAINT_PWD:?}"
[[ "$DB_SETTINGS_PARTITION_MAINT_USER" == dba_partition_maint ]] || { echo 'Unexpected maintenance account' >&2; exit 78; }
export PGHOST="$DB_SETTINGS_PIPELINE_DB_HOST" PGPORT="$DB_SETTINGS_PIPELINE_DB_PORT"
export PGDATABASE="$DB_SETTINGS_PRD_RAW_DB_NAME" PGUSER="$DB_SETTINGS_PARTITION_MAINT_USER"
export PGPASSWORD="$DBA_PARTITION_MAINT_PWD"
export PGOPTIONS='-c timezone=UTC -c lock_timeout=5000'
PSQL=(psql -X -v ON_ERROR_STOP=1 -At)
mapfile -t parts < <("${PSQL[@]}" <<'SQL'
SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
WHERE i.inhparent='public.gametransaction_prod'::regclass
AND c.relnamespace='public'::regnamespace
AND c.relname ~ '^gametransaction_prod_y[0-9]{4}m(0[1-9]|1[0-2])$'
ORDER BY c.relname;
SQL
)
count=${#parts[@]}
if (( count <= 7 )); then echo "[RETENTION SKIP] $count partition(s), minimum is 7"; exit 0; fi
if [[ "${PRISM_PARTITION_DROP_ENABLED:-NO}" != YES ]]; then
  echo "[RETENTION DISABLED] $count partitions; candidate(s): $((count-7)). No backup or removal performed."; exit 0
fi
# An enabled run requires a second explicit SQL permission grant, intentionally omitted.
command -v pg_dump >/dev/null; command -v pg_restore >/dev/null; command -v sha256sum >/dev/null
ARCHIVE_DIR="${PRISM_PARTITION_ARCHIVE_DIR:-$ROOT/partition_archives}"
mkdir -p "$ARCHIVE_DIR"; chmod 700 "$ARCHIVE_DIR"
# Run one retention job at a time; ensure the backup host has sufficient capacity.
exec 9>"$ARCHIVE_DIR/.retention.lock"
flock -n 9 || { echo '[RETENTION SKIP] Another retention run is active'; exit 0; }
for ((i=0; i<count-7; i++)); do
  name="${parts[i]}"
  month="${name##*_y}"; ym="${month/m/}"
  now_month="$(date -u +%Y%m)"
  if [[ "$ym" == "$now_month" || "$ym" > "$now_month" ]]; then echo "[RETENTION SKIP] $name current/future"; continue; fi
  # Check month remains among oldest candidates before backup.
  archive="$ARCHIVE_DIR/${name}_$(date -u +%Y%m%dT%H%M%SZ)_$$.dump"
  partial="${archive}.partial"
  trap 'rm -f -- "$partial"' EXIT
  echo "[BACKUP] $name -> $archive"
  pg_dump --format=custom --no-owner --no-acl --table="public.$name" --file="$partial"
  [[ -s "$partial" ]] || { echo '[ERROR] Empty backup' >&2; exit 1; }
  pg_restore --list "$partial" > /dev/null
  mv -- "$partial" "$archive"
  (cd "$ARCHIVE_DIR" && sha256sum "$(basename "$archive")" > "$(basename "$archive").sha256" && sha256sum -c "$(basename "$archive").sha256")
  # Archive integrity does not guarantee restorability. Run restore drill before production approval.
  result="$("${PSQL[@]}" -v target="$name" <<'SQL'
SELECT public.retire_gametx_partition(:'target');
SQL
)"
  echo "$result"
  [[ "$result" == *'[PARTITION RETIRED]'* ]] || { echo '[RETENTION STOP] Target not retired after backup; review before continuing'; exit 1; }
  trap - EXIT
done

