#!/usr/bin/env bash

# INITIAL ENTRY POINT: load non-secret config.toml and shared .env only once.
# All child Bash/Python handoffs inherit the exported environment.
source "$HOME/scripts/prism/common/config/load_config.sh" || exit 1

# The original manually entered dates remain available for backfills:
# START_DATE="2024-11-04"
# END_DATE="2024-11-05"
WINDOW_FROM=$(date +%F)
WINDOW_TO=$(date -d "tomorrow" +%F)

CURRENT_DATE="$WINDOW_FROM"

# Existing extractor script location (adjust only if the script was moved to d2s).
EXTRACTOR_SCRIPT="$DIRECT2SILVER_PHASE/egms_gametx_inplayv1_mysql_extractor.py"
# Use the Python interpreter from the user's existing virtual environment.
PYTHON_BIN="$HOME/scripts/.venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "[ERROR] Python virtual environment not found: $PYTHON_BIN" >&2
    exit 1
fi
if [[ ! -f "$EXTRACTOR_SCRIPT" ]]; then
    echo "[ERROR] Extractor script not found: $EXTRACTOR_SCRIPT" >&2
    exit 1
fi

while [[ "$CURRENT_DATE" < "$WINDOW_TO" ]]; do
    NEXT_DATE=$(date -d "$CURRENT_DATE +1 day" +"%Y-%m-%d")

    # Do not go beyond the actual WINDOW_TO
    if [[ "$NEXT_DATE" > "$WINDOW_TO" ]]; then
        NEXT_DATE="$WINDOW_TO"
    fi

    FROM_DATE="${CURRENT_DATE} 06:00:00"
    TO_DATE="${NEXT_DATE} 06:00:00"

    echo "======================================================"
    echo "Running daily batch"
    echo "WINDOW_FROM : $WINDOW_FROM"
    echo "WINDOW_TO   : $WINDOW_TO"
    echo "FROM        : $FROM_DATE"
    echo "TO          : $TO_DATE"
    echo "======================================================"

    "$PYTHON_BIN" "$EXTRACTOR_SCRIPT" \
        --table "$DB_SETTINGS_INPLAY1_GAMETX_TABLENAME" \
        --pwd "$INPLAYV1_SRC_PWD" \
        --from "$FROM_DATE" \
        --to "$TO_DATE" \
        --date-column "$DB_SETTINGS_INPLAY1_GAMETX_BUSINESS_WINDOW_DATE"

    PYPROC=$?
    if [[ "$PYPROC" -ne 0 ]]; then
        echo "FAILED: $CURRENT_DATE" >&2
        echo "Exit code: $PYPROC" >&2
        exit "$PYPROC"
    fi

    echo "SUCCESS: $CURRENT_DATE"
    echo
    CURRENT_DATE="$NEXT_DATE"
done

echo "======================================================"
echo "All daily batches completed successfully."
# echo "======================================================"
