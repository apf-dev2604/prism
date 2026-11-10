#!/usr/bin/env bash
# PRISM Gold Phase multi-brand strict runner. Retains staged DQ/publish gates.
set -Eeuo pipefail
# PRISM centralized configuration (config.toml + common/.env).
source "$HOME/scripts/prism/common/config/load_config.sh" || exit 1
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RAW_VENV="${VENV_DIR:-$HOME/scripts/.venv}"
VENV_DIR="${RAW_VENV/#\~/$HOME}"
PYTHON="${VENV_DIR}/bin/python"
SHARED="${SHARED_MODS:-$HOME/scripts/prism/common/shared_modules}"
mkdir -p "$ROOT/logs" "$ROOT/work/reports"
usage() {
  echo "Usage: bash run_imp_nexusall.sh FROM TO gameTransaction|walletTransaction [--brand BRAND|--brands BRAND|-brands BRAND] [--jmp-task dqstgprd|loadstg|loadstgprd|posteval | --resume-from staging|dq|publish]" >&2
}
# Backward-compatible positional dates/entity. Brand optional (defaults to V1 for D2S).
(( $# >= 3 )) || { usage; exit 2; }
FROM="$1"; TO="$2"; ENTITY="$3"; shift 3
BRAND="inplayv1"
BRAND_SPECIFIED=0
EXTRA=()
MODE="full"
while (( $# )); do
  case "$1" in
    --brand|--brands|-brand|-brands)
      (( $# >= 2 )) || { usage; exit 2; }
      (( BRAND_SPECIFIED == 0 )) || { echo '[ERROR] Duplicate brand flag' >&2; exit 2; }
      BRAND="$2"; BRAND_SPECIFIED=1; shift 2 ;;
    --resume-from)
      (( $# >= 2 )) || { usage; exit 2; }
      [[ ${#EXTRA[@]} -eq 0 && "$MODE" == full ]] || { usage; exit 2; }
      case "$2" in staging|dq|publish) EXTRA=(--resume-from "$2");; *) usage; exit 2;; esac
      shift 2 ;;
    --jmp-task)
      (( $# >= 2 )) || { usage; exit 2; }
      [[ ${#EXTRA[@]} -eq 0 && "$MODE" == full ]] || { usage; exit 2; }
      case "$2" in dqstgprd|loadstg|loadstgprd|posteval) MODE="$2";; *) usage; exit 2;; esac
      shift 2 ;;
    *) echo "[ERROR] Unrecognized option $1" >&2; usage; exit 2;;
  esac
done
[[ "$BRAND" =~ ^[a-zA-Z][a-zA-Z0-9_]*$ ]] || { echo 'Invalid brand key' >&2; exit 2; }
[[ "$FROM" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ && "$TO" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { usage; exit 2; }
case "$ENTITY" in gameTransaction|walletTransaction) ;; *) usage; exit 2;; esac
[[ -x "$PYTHON" ]] || { echo "[ERROR] Python virtualenv missing: $PYTHON" >&2; exit 2; }
[[ -f "$SHARED/etl_alert.py" ]] || { echo "[ERROR] Shared alert missing: $SHARED/etl_alert.py" >&2; exit 78; }
cd "$ROOT"
# Validate adapter and configuration before any DDL/DML. Normalize V2 alias
# (inplayv2) to the persisted audit brand key (inplay).
BRAND="$("$PYTHON" -c 'import sys; from helpers.main import load_brand; print(load_brand(sys.argv[1])[1].BRAND_KEY)' "$BRAND")" || exit 2
# DEV InplayV2 posteval uses the same deployed Gold Phase directory.
if [[ "$BRAND" == inplay ]]; then
  export GOLD_PHASE="$ROOT"
fi
if [[ "$BRAND" == "inplay" && "$ENTITY" == "walletTransaction" ]]; then
  echo '[NOT AVAILABLE YET] inplayv2 walletTransaction (deposits/withdrawals)' >&2
  exit 78
fi
if [[ "$BRAND" == "inplayv1" ]]; then
  echo '[BRAND ROUTE] InplayV1 | D2S full ETL | fulletl=TRUE | tableRoomId=source Outlet'
else
  echo "[BRAND ROUTE] $BRAND | Gold-only ETL | fulletl=FALSE | tableRoomId=adapter-mapped"
fi
# All modes are available; strict pipeline enforces DQ approval before publication.
# Child tracebacks/progress go to RUN_LOG. Console shows concise phase status.
cd "$ROOT"
RUN_LOG="$ROOT/logs/nexus_${BRAND}_${ENTITY}_${FROM}_${TO}_$(date +%Y%m%d_%H%M%S)_$$.log"
# Keep details and tracebacks in the run log; print one useful clue on failure.
on_exit() {
  local rc=$?
  trap - EXIT
  if (( rc != 0 )); then
    local clue
    clue="$(grep -E '(^|[[:space:]])(argparse\.ArgumentError:|psycopg2\.errors\.[[:alnum:]_]+:|[[:alnum:]_]*(Error|Exception):|BLOCK:|\[ERROR\]|\[BLOCKED\])' "$RUN_LOG" 2>/dev/null | tail -n 1 || true)"
    [[ -n "$clue" ]] || clue="See run log for full traceback"
    echo "[NEXUS ERROR] phase=$MODE rc=$rc | $clue" >&2
    echo "[NEXUS LOG] $RUN_LOG" >&2
    printf '%s | ERROR | phase=%s rc=%s | %s\n' "$(date '+%F %T')" "$MODE" "$rc" "$clue" >> "$RUN_LOG"
    "$PYTHON" "$SHARED/etl_alert.py" --stage DLAKE-NEXUS --entity "$ENTITY" --window-from "$FROM" --window-to "$TO" --exit-code "$rc" --error-log "$RUN_LOG" >> "$RUN_LOG" 2>&1 || true
  fi
  exit "$rc"
}
trap on_exit EXIT

if [[ "$MODE" == posteval ]]; then
  if [[ "$ENTITY" != gameTransaction ]]; then
    echo '[BLOCKED] posteval is currently verified only for gameTransaction.' >&2
    exit 78
  fi
  echo '[NEXUS POSTEVAL ONLY] Post-release verification; no loading, DQ replay or publishing.' | tee -a "$RUN_LOG"
  bash "$ROOT/run_post_etl_verification.sh" "$FROM" "$TO" "$ENTITY" "$BRAND" >> "$RUN_LOG" 2>&1

elif [[ "$MODE" == dqstgprd ]]; then
  echo '[NEXUS DQ ONLY] Validate existing staging; no production publishing.' | tee -a "$RUN_LOG"
  "$PYTHON" -m helpers.strict_pipeline --date-from "$FROM" --date-to "$TO" \
    --entity "$ENTITY" --brand "$BRAND" --resume-from dq --dq-only >> "$RUN_LOG" 2>&1

elif [[ "$MODE" == loadstg ]]; then
  echo '[NEXUS LOAD ONLY] Staging load; no DQ or production publishing.' | tee -a "$RUN_LOG"
  "$PYTHON" -m helpers.strict_pipeline \
    --date-from "$FROM" \
    --date-to "$TO" \
    --entity "$ENTITY" \
    --brand "$BRAND" \
    --stage-only >> "$RUN_LOG" 2>&1

else
  if [[ "$MODE" == loadstgprd ]]; then
    EXTRA=(--resume-from publish)
  fi
  if [[ "$ENTITY" == gameTransaction && ( "$MODE" == full || "$MODE" == loadstgprd ) && ( ${#EXTRA[@]} -eq 0 || "${EXTRA[*]}" == "--resume-from publish" ) ]]; then
    echo '[PARTITION PREFLIGHT] Preparing production monthly partition(s).' | tee -a "$RUN_LOG"
    bash "$ROOT/gametxprod_partition_maker.sh" "$FROM" "$TO" >> "$RUN_LOG" 2>&1
  fi
  "$PYTHON" -m helpers.strict_pipeline \
    --date-from "$FROM" \
    --date-to "$TO" \
    --entity "$ENTITY" \
    --brand "$BRAND" \
    "${EXTRA[@]}" >> "$RUN_LOG" 2>&1

  # Only hand off after this execution explicitly confirms an atomic release.
  # This prevents posteval on resume-from staging if it stops at STAGING_COMPLETE.
  if [[ "$ENTITY" == gameTransaction ]] && grep -Fq "[STRICT RELEASED] entity=$ENTITY " "$RUN_LOG"; then
    echo '[PRISM HANDOFF] STRICT RELEASED confirmed; starting post-release verification.' | tee -a "$RUN_LOG"
    bash "$ROOT/run_post_etl_verification.sh" "$FROM" "$TO" "$ENTITY" "$BRAND" >> "$RUN_LOG" 2>&1
    echo '[PRISM HANDOFF] Post-release verification PASSED.' | tee -a "$RUN_LOG"
  else
    echo '[PRISM HANDOFF] Post-release verification not started (no supported gameTransaction STRICT RELEASED marker in this execution).' | tee -a "$RUN_LOG"
  fi
fi


# The detailed execution transcript is saved in RUN_LOG (not mirrored to stdout).
echo "[NEXUS OK] phase=$MODE completed | log=$RUN_LOG"
