#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
export TZ=Asia/Manila

# PRISM InplayV2 afternoon backfill
# Start new business windows ONLY between 12:00:00 and 12:59:59 UTC
# (20:00-20:59:59 PHT). An in-flight window is never interrupted.
# Business dates: 2026-06-01 through 2026-09-30 (end exclusive 2026-10-01).
# Each window invokes the COMPLETE InplayV2 Gold-phase controller:
#   bash run_imp_nexusall.sh FROM TO gameTransaction --brands inplayv2
# The controller MUST return nonzero on Gold DQ, publishing, or posteval failure.

VENV_DIR="${PRISM_VENV_DIR:-/home/allanf/scripts/.venv}"
PYTHON_BIN="$VENV_DIR/bin/python"
BASE_DIR="${PRISM_BASE_DIR:-/home/allanf/scripts/prism}"
GOLD_DIR="$BASE_DIR/gold-phase"
GOLD_CONTROLLER="${PRISM_GOLD_CONTROLLER:-$GOLD_DIR/run_imp_nexusall.sh}"
LOG_DIR="${PRISM_LOG_DIR:-$BASE_DIR/cron_logs}"
STATE_DIR="${PRISM_STATE_DIR:-$BASE_DIR/backfill_state}"

FROM=2026-06-01
TO=2026-10-01
BRAND=inplayv2
ENTITY=gameTransaction
DRY_RUN=0
SHOW_STATUS=0

usage() {
  cat <<'USAGE'
Usage: bash afternoon_backfill_inplayv2.sh [--dry-run] [--status] [--help]

Fixed coverage: 2026-06-01 to 2026-09-30 inclusive.
Fixed route: inplayv2 Gold-only; logical brand=inplay, fulletl=FALSE.
Start time: 12:00-13:00 UTC every day (20:00-21:00 Asia/Manila).
Only starts NEW complete daily business windows before 13:00 UTC.
An in-flight controller is never terminated at cutoff.
Saved checkpoint advances only when controller exits successfully (0).

Cron (when daemon schedules in UTC): 0 12 * * *
USAGE
}

while (( $# )); do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --status) SHOW_STATUS=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) printf '[ERROR] Unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

valid_date() {
  [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] &&
    [[ "$(date -u -d "$1" +%F 2>/dev/null)" == "$1" ]]
}

inside_run_window() {
  local hhmm
  hhmm="$(TZ=UTC date +%H%M)"
  (( 10#$hhmm >= 1200 && 10#$hhmm < 1300 ))
}

# A 06:00 PHT -> next-day 06:00 PHT business window can be used
# only once its END boundary is in the past. At 12:00 UTC today,
# that boundary has already passed for the current PHT calendar date.
completed_boundary() {
  local pht_today pht_hhmm
  pht_today="$(TZ=Asia/Manila date +%F)"
  pht_hhmm="$(TZ=Asia/Manila date +%H%M)"
  if (( 10#$pht_hhmm < 600 )); then
    date -u -d "$pht_today - 1 day" +%F
  else
    printf '%s\n' "$pht_today"
  fi
}

mkdir -p "$LOG_DIR" "$STATE_DIR"
KEY="afternoon_${BRAND}_${FROM//-/}_${TO//-/}"
STATE_FILE="$STATE_DIR/${KEY}.next"
LOCK_FILE="$STATE_DIR/${KEY}.lock"

# Prevent duplicate afternoon controllers, including cron/manual overlap.
exec 9>"$LOCK_FILE"
flock -n 9 || { echo '[SKIP] Afternoon backfill already running.'; exit 0; }

NEXT="$FROM"
if [[ -s "$STATE_FILE" ]]; then
  IFS= read -r NEXT < "$STATE_FILE"
  if ! valid_date "$NEXT" || [[ "$NEXT" < "$FROM" || "$NEXT" > "$TO" ]]; then
    echo "[ERROR] Invalid checkpoint $NEXT in $STATE_FILE" >&2
    exit 1
  fi
fi

if (( SHOW_STATUS )); then
  echo "[STATUS] Brand=$BRAND; Next=$NEXT; EndExclusive=$TO"
  echo "[STATUS] StateFile=$STATE_FILE"
  exit 0
fi

if (( ! DRY_RUN )); then
  if ! inside_run_window; then
    echo "[SKIP] Outside 12:00-13:00 UTC. Saved next window: $NEXT"
    exit 0
  fi
  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "[ERROR] Missing Python virtual environment: $PYTHON_BIN" >&2
    exit 1
  fi
  if [[ ! -f "$GOLD_CONTROLLER" ]]; then
    echo "[ERROR] Missing Gold controller: $GOLD_CONTROLLER" >&2
    exit 1
  fi
  export VIRTUAL_ENV="$VENV_DIR"
  export PATH="$VENV_DIR/bin:$PATH"
  echo "[PRISM PYTHON] Executable: $PYTHON_BIN"
  "$PYTHON_BIN" --version
fi

write_checkpoint() {
  local tmp="$STATE_FILE.tmp.$$"
  printf '%s\n' "$1" > "$tmp"
  mv -f "$tmp" "$STATE_FILE"
}

# Use a dedicated, shared lock path for Gold-phase work. This prevents
# overlapping copies of THIS controller and other controllers only IF they
# also acquire the same lock. Update the nightly script to use this lock.
GOLD_SHARED_LOCK="${PRISM_GOLD_SHARED_LOCK:-$STATE_DIR/prism_gold_phase.shared.lock}"
exec 8>"$GOLD_SHARED_LOCK"

printf '[PLAN] Adapter=%s | LogicalBrand=inplay | fulletl=FALSE | From=%s | EndExclusive=%s | Resume=%s\n' \
  "$BRAND" "$FROM" "$TO" "$NEXT"

while [[ "$NEXT" < "$TO" ]]; do
  if (( ! DRY_RUN )) && ! inside_run_window; then
    echo "[PAUSE] 13:00 UTC cutoff reached. Next date saved: $NEXT"
    exit 0
  fi

  DAY="$NEXT"
  END="$(date -u -d "$DAY + 1 day" +%F)"
  MAX_COMPLETE="$(completed_boundary)"
  if [[ "$END" > "$MAX_COMPLETE" ]]; then
    echo "[PAUSE] Source business window not complete: $DAY -> $END"
    exit 0
  fi

  CMD=(bash "$GOLD_CONTROLLER" "$DAY" "$END" "$ENTITY" --brands "$BRAND")
  LOG_FILE="$LOG_DIR/afternoon_${BRAND}_${DAY}_${END}_full.log"

  printf '[RUN] %s -> %s | ' "$DAY" "$END"
  printf '%q ' "${CMD[@]}"
  printf '\n'

  if (( DRY_RUN )); then
    NEXT="$END"
    continue
  fi

  # Do not compete with a cooperating nightly job for the shared staging table.
  if ! flock -n 8; then
    echo "[PAUSE] Another Gold-phase controller holds $GOLD_SHARED_LOCK; next=$DAY"
    exit 0
  fi

  # Run controller from its directory so relative paths used by its scripts work.
  if ( cd "$GOLD_DIR" && "${CMD[@]}" ) > "$LOG_FILE" 2>&1; then
    echo "[PROCESS] Controller exit=0; log=$LOG_FILE"
  else
    rc=$?
    flock -u 8
    echo "[FAIL] Controller exit=$rc; checkpoint stays at $DAY; log=$LOG_FILE" >&2
    exit "$rc"
  fi
  flock -u 8

  # Controller success is only safe if it faithfully propagates failures.
  # It must report Gold DQ / publish / post-release failures as nonzero.
  write_checkpoint "$END"
  NEXT="$END"
  echo "[CHECKPOINT] Completed $DAY -> $END; next=$NEXT"
done

echo "[COMPLETE] All InplayV2 windows through 2026-09-30 processed successfully."

