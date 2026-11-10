
#!/usr/bin/env bash
set -Eeuo pipefail

umask 077
export TZ="Asia/Manila"

# --------------------------------------------------
# SCHEDULE SETTINGS
# --------------------------------------------------

# First business window starts on this date.
ANCHOR_DATE="2024-12-29"

# December 29 to January 9 = 11 elapsed days.
BATCH_DAYS=11

# Existing controller location.
# Replace this filename with the actual filename
# of the Bash controller you provided.
CONTROLLER="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/run_full_etl.sh"

# Log directory
LOG_DIR="$HOME/scripts/prism/cron_logs"
mkdir -p "$LOG_DIR"

# --------------------------------------------------
# DATE CALCULATION
# --------------------------------------------------

TODAY=$(date +%F)

ANCHOR_EPOCH=$(date -u -d "$ANCHOR_DATE" +%s)
TODAY_EPOCH=$(date -u -d "$TODAY" +%s)

DAYS_ELAPSED=$(( (TODAY_EPOCH - ANCHOR_EPOCH) / 86400 ))

# Only process completed windows.
if (( DAYS_ELAPSED < BATCH_DAYS )); then
    exit 0
fi

# Execute only every BATCH_DAYS.
if (( DAYS_ELAPSED % BATCH_DAYS != 0 )); then
    exit 0
fi

# Use the just-completed window.
WINDOW_FROM=$(
  date -u -d "$TODAY - $BATCH_DAYS days" +%F
)

WINDOW_TO="$TODAY"

# --------------------------------------------------
# EXECUTION
# --------------------------------------------------

LOG_FILE="$LOG_DIR/etl_${WINDOW_FROM}_${WINDOW_TO}.log"

echo "======================================"
echo "PRISM SCHEDULED FULL ETL"
echo "Execution Date : $TODAY"
echo "Window From    : $WINDOW_FROM"
echo "Window To      : $WINDOW_TO"
echo "Mode           : full"
echo "======================================"

if [[ ! -f "$CONTROLLER" ]]; then
    echo "[ERROR] Controller missing: $CONTROLLER" >&2
    exit 1
fi


# Separate wrapper lock.
# Original controller retains its own flock.
exec 8>"$LOG_DIR/cron_wrapper.lock"

flock -n 8 || {
    echo "[ERROR] Scheduled wrapper already active." >&2
    exit 75
}

# ---------------------------------------------
# EXECUTE EXISTING CONTROLLER
# ---------------------------------------------

if bash "$CONTROLLER" \
    --from "$WINDOW_FROM" \
    --to "$WINDOW_TO" \
    --jmp-task full \
    --brands "${PRISM_D2S_BRAND:-inplayv1}" \
    >> "$LOG_FILE" 2>&1
then

    echo "[PASS] ETL completed successfully."
    echo "[PASS] Window: $WINDOW_FROM -> $WINDOW_TO"

    # ---------------------------------------------
    # POST-SUCCESS DUMP CLEANUP - DRY RUN
    # ---------------------------------------------

    DUMP_ROOT="/home/allanf/scripts/prism/d2s-phase/daily_dmp/jsonl"

    echo "[CLEANUP] ETL batch completed successfully."
    echo "[CLEANUP] Checking old dump files."

    if [[ -d "$DUMP_ROOT" ]]; then
        find "$DUMP_ROOT" \
            -type f \
            -name "*.jsonl.gz" \
            -mtime +5 \
            #-print
	    -delete
    else
        echo "[WARNING] Dump directory not found: $DUMP_ROOT"
    fi

else
    RC=$?

    echo "[FAIL] ETL failed with exit code $RC."
    echo "[SKIP] Dump cleanup skipped."

    exit "$RC"
fi



