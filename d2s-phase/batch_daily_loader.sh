#!/usr/bin/env bash
# PRISM - MySQL -> PostgreSQL Data Lake (RAW DATA IMPORT)
# Automatic: extractor passes FILE FROM TO STATUS_FILE STARTED_FILE.
# Manual: bash batch_daily_loader.sh [WINDOW_FROM WINDOW_TO].
# Existing PostgreSQL load and DQ handoff stay in the Python loader.

set -Eeuo pipefail
umask 077

# Load the same non-secret TOML and protected .env as the extractor.
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/../common/config/load_config.sh" || exit 1

BASE_DIR="$DIRECT2SILVER_PHASE"
PYTHON_BIN="${HOME}/scripts/.venv/bin/python"
LOADER_PY="${BASE_DIR}/egms_gametx_inplayv1_pg_loader.py"
DB_SCHEMA="$DB_SETTINGS_DW_RAW_SCHEMA_NAME"
DB_TABLENAME="$DB_SETTINGS_DW_RAW_DB_TABLENAME"
HISTORY_SCHEMA="$DB_SETTINGS_DW_RAW_SCHEMA_NAME"
HISTORY_TABLE="temp_egms_games_txn_history"
LOAD_MODE="insert-only"
SKIP_HISTORY="true"
NICE_LEVEL="$COMMON_ETL_SETTINGS_OS_NICE_INCREMENT"
LOADER_LOCK_FILE="${BASE_LOG_DIR}/egms_pg_loader.lock"
DB_NAME="$DB_SETTINGS_DW_RAW_DB_NAME"


# Original five-argument handoff must be preserved exactly.
if [[ "$#" -eq 5 ]]; then
    JSONL_FILE="$1"
    FROM_DATE="$2"
    TO_DATE="$3"
    STATUS_FILE="$4"
    STARTED_FILE="$5"
elif [[ "$#" -eq 0 || "$#" -eq 2 ]]; then
    # Manual mode: one 06:00-to-next-day-06:00 business window.
    WINDOW_FROM="${1:-$(date +%F)}"
    WINDOW_TO="${2:-$(date -d 'tomorrow' +%F)}"
    FROM_DATE="${WINDOW_FROM} 06:00:00"
    TO_DATE="${WINDOW_TO} 06:00:00"
    LOAD_DATE="${WINDOW_FROM//-/}"
    shopt -s nullglob
   # files=("${DMP_DIR}/${LOAD_DATE:0:6}/${DB_SETTINGS_INPLAY1_FILE_PREFIX}_${LOAD_DATE}_exractdt_"*.jsonl.gz)
    files=("${DMP_DIR}/${LOAD_DATE:0:6}/${LOAD_DATE}/${DB_SETTINGS_INPLAY1_FILE_PREFIX}_${LOAD_DATE}_exractdt_"*.jsonl.gz)  
    shopt -u nullglob
    if [[ "${#files[@]}" -ne 1 ]]; then
        printf '[ERROR] JSON_DUMP Extraction Dir for %s; found %s.\n' "$LOAD_DATE" "${#files[@]}" >&2
        exit 2
    fi
    JSONL_FILE="${files[0]}"
    STATUS_FILE="${BASE_LOG_DIR}/loader_manual_${LOAD_DATE}.status"
    STARTED_FILE="${BASE_LOG_DIR}/loader_manual_${LOAD_DATE}.started"
else
    echo 'Usage: batch_daily_loader.sh [WINDOW_FROM WINDOW_TO]' >&2
    echo 'Handoff: batch_daily_loader.sh FILE FROM TO STATUS_FILE STARTED_FILE' >&2
    exit 2
fi

PYTHON_STARTED=false

write_final_status() {
    local rc=$?
    trap - EXIT
    local result="FAIL"
    [[ "$rc" -eq 0 ]] && result="PASS"
    mkdir -p "$(dirname "$STATUS_FILE")"
    printf '%s\t%s\t%s\n' "$result" "$rc" "$(date -Is)" > "${STATUS_FILE}.tmp"
    mv -f "${STATUS_FILE}.tmp" "$STATUS_FILE"
    # Pre-Python failure notification uses the same shared SMTP module.
    # Python loader notifies independently if it already started.
    if [[ "$rc" -ne 0 && "$PYTHON_STARTED" == false ]]; then
        "$PYTHON_BIN" "$SHARED_MODS/etl_alert.py" \
            --stage MYSQL-DLAKE-PG-LOADER \
            --entity gameTransaction \
            --window-from "$FROM_DATE" --window-to "$TO_DATE" \
            --exit-code "$rc" || true
    fi
}
trap write_final_status EXIT

: "${ETL_DB_USER_PWD:?ETL_DB_USER_PWD missing: configure common/.env}"
[[ -x "$PYTHON_BIN" ]] || { echo "Python missing: $PYTHON_BIN" >&2; exit 2; }
[[ -s "$JSONL_FILE" ]] || { echo "Missing/empty JSONL: $JSONL_FILE" >&2; exit 2; }
[[ -f "$LOADER_PY" ]] || { echo "Missing loader Python: $LOADER_PY" >&2; exit 2; }
[[ "$FROM_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ 06:00:00$ ]] || { echo "Invalid FROM_DATE" >&2; exit 2; }
[[ "$TO_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ 06:00:00$ ]] || { echo "Invalid TO_DATE" >&2; exit 2; }
expected_to="$(date -d "${FROM_DATE:0:10} +1 day" +%F) 06:00:00"
[[ "$TO_DATE" == "$expected_to" ]] || { echo "Expected TO_DATE: $expected_to" >&2; exit 2; }
[[ "$LOAD_MODE" == 'upsert' || "$LOAD_MODE" == 'insert-only' ]] || { echo "Invalid LOAD_MODE" >&2; exit 2; }

mkdir -p "$(dirname "$LOADER_LOCK_FILE")" "$(dirname "$STARTED_FILE")"
exec 9>"$LOADER_LOCK_FILE"
flock -n 9 || { echo "Another PG loader owns the staging-table lock" >&2; exit 75; }
LOAD_DATE="${FROM_DATE:0:10}"
LOAD_DATE="${LOAD_DATE//-/}"

printf '%s | INFO | Phase          : MySQL -> Data Lake RAW DATA IMPORT\n' "$(date '+%F %T')"
printf '%s | INFO | Database       : %s\n' "$(date '+%F %T')" "$DB_SETTINGS_DW_RAW_DB_NAME"
printf '%s | INFO | Destination    : %s.%s\n' "$(date '+%F %T')" "$DB_SCHEMA" "$DB_TABLENAME"
printf '%s | INFO | Source file    : %s\n' "$(date '+%F %T')" "$JSONL_FILE"
printf '%s | INFO | Window from UTC: %s\n' "$(date '+%F %T')" "$FROM_DATE"
printf '%s | INFO | Window to UTC  : %s\n' "$(date '+%F %T')" "$TO_DATE"

args=(--date "$LOAD_DATE" --file "$JSONL_FILE" --pg-password "$ETL_DB_USER_PWD" --final
      --to-schema "$DB_SCHEMA" --to-table "$DB_TABLENAME"
      --history-schema "$HISTORY_SCHEMA" --history-table "$HISTORY_TABLE"
      --mode "$LOAD_MODE")
[[ "$SKIP_HISTORY" == 'true' ]] && args+=(--skip-history)
printf 'PYTHON_LAUNCH_REQUESTED\t%s\n' "$(date -Is)" > "$STARTED_FILE"
PYTHON_STARTED=true
nice -n "$NICE_LEVEL" "$PYTHON_BIN" "$LOADER_PY" "${args[@]}"
echo "PG LOADER PASS: $LOAD_DATE"

