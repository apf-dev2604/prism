
#!/usr/bin/env bash
# Standalone InplayV1 Data Lake -> NEXUS DQ (no migration).
set -Eeuo pipefail
# PRISM centralized configuration (config.toml + common/.env).
source "$HOME/scripts/prism/common/config/load_config.sh" || exit 1
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RAW_VENV="${VENV_DIR:-$HOME/scripts/.venv}"
VENV_DIR="${RAW_VENV/#\~/$HOME}"
PYTHON="${VENV_DIR}/bin/python"
SHARED="${SHARED_MODS:-$HOME/scripts/prism/common/shared_modules}"
mkdir -p "$ROOT/logs" "$ROOT/work/reports"
if [[ $# -ne 4 ]]; then
  echo "Usage: bash $(basename "$0") DATE_FROM DATE_TO {gameTransaction|walletTransaction} BRAND" >&2
  exit 2
fi
FROM="$1"; TO="$2"; ENTITY="$3"; BRAND="$4"
#if [[ ! "$FROM" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}$ || ! "$TO" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
if [[ ! "$FROM" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ || ! "$TO" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo 'ERROR: Dates must be YYYY-MM-DD.' >&2; exit 2
fi
case "$ENTITY" in gameTransaction|walletTransaction) ;; *) echo "ERROR: Unsupported entity '$ENTITY'" >&2; exit 2;; esac
[[ -d "$ROOT/helpers" ]] || { echo 'ERROR: Missing helpers directory' >&2; exit 2; }
[[ -x "$PYTHON" ]] || { echo "ERROR: Python not executable: $PYTHON" >&2; exit 2; }
cd "$ROOT"
# Running directly: capture stdout/stderr and alert. When invoked from importer,
# importer owns notification and has captured this output in the parent log.
if [[ "${NEXUS_ALERT_OWNER:-}" != importer ]]; then
  RUN_LOG="$ROOT/logs/nexus_dq_${ENTITY}_${FROM}_${TO}_$(date +%Y%m%d_%H%M%S)_$$.log"
  on_exit() {
    local rc=$?
    trap - EXIT
    if (( rc != 0 )); then
      echo "$(date '+%F %T') | ERROR | NEXUS DQ rc=$rc | log=$RUN_LOG" | tee -a "$RUN_LOG" >&2
      "$PYTHON" "$SHARED/etl_alert.py" --stage DLAKE-NEXUS-DQ --entity "$ENTITY" --window-from "$FROM" --window-to "$TO" --exit-code "$rc" --error-log "$RUN_LOG" || true
    fi
    exit "$rc"
  }
  trap on_exit EXIT
  echo "[NEXUS STANDALONE DQ] entity=$ENTITY from=$FROM to=$TO" | tee -a "$RUN_LOG"
  "$PYTHON" -m helpers.dq_reconcile --brand "$BRAND" --entity "$ENTITY" --date-from "$FROM" --date-to "$TO" --chunk-size "${DQ_CHUNK_SIZE:-1000}" 2>&1 | tee -a "$RUN_LOG"
else
  echo "[NEXUS STANDALONE DQ] entity=$ENTITY from=$FROM to=$TO"
  "$PYTHON" -m helpers.dq_reconcile --brand "$BRAND" --entity "$ENTITY" --date-from "$FROM" --date-to "$TO" --chunk-size "${DQ_CHUNK_SIZE:-1000}"
fi

