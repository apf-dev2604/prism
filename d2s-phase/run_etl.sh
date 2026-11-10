#!/usr/bin/env bash
# PRISM - Single entry point for full ETL or an existing stage.
# Existing extraction, loader and DQ scripts retain their processing logic.
set -Eeuo pipefail

source "$HOME/scripts/prism/common/config/load_config.sh" || exit 1

JMP_TASK="full"
BRAND="${PRISM_D2S_BRAND:-inplayv1}"
ENTITY="gameTransaction"
DATES=()
while (( $# > 0 )); do
    case "$1" in
        --brand|--brands|-brand|-brands)
            (( $# >= 2 )) || { echo '[ERROR] Brand value required' >&2; exit 2; }
            BRAND="$2"; shift 2 ;;
        --entity)
            (( $# >= 2 )) || { echo '[ERROR] Entity value required' >&2; exit 2; }
            ENTITY="$2"; shift 2 ;;
        --jmp-task)
            if (( $# < 2 )); then
                echo "[ERROR] --jmp-task requires all, load, loaddq or rawdq" >&2
                exit 2
            fi
            JMP_TASK="$2"
            shift 2
            ;;
        --jmp-task-all)
            JMP_TASK="all"
            shift
            ;;
        --*)
            echo "[ERROR] Unknown option: $1" >&2
            exit 2
            ;;
        *)
            DATES+=("$1")
            shift
            ;;
    esac
done

if (( ${#DATES[@]} != 0 && ${#DATES[@]} != 2 )); then
    echo 'Usage: run_prism.sh [YYYY-MM-DD YYYY-MM-DD] [--jmp-task all|load|loaddq|rawdq]' >&2
    exit 2
fi

WINDOW_FROM="${DATES[0]:-$(date +%F)}"
WINDOW_TO="${DATES[1]:-$(date -d 'tomorrow' +%F)}"
[[ "$BRAND" =~ ^[a-zA-Z][a-zA-Z0-9_]*$ ]] || { echo '[ERROR] Invalid brand key' >&2; exit 2; }
[[ "$ENTITY" == gameTransaction || "$ENTITY" == walletTransaction ]] || { echo '[ERROR] Invalid entity' >&2; exit 2; }
if [[ "$ENTITY" == walletTransaction ]]; then
    echo "[NOT AVAILABLE YET] $BRAND walletTransaction (deposits/withdrawals)"
    exit 0
fi
export PRISM_D2S_BRAND="$BRAND"
if [[ "$BRAND" != inplayv1 ]]; then
    # Gold-only brands must not run InplayV1 MySQL extractor/Raw DQ.
    GOLD_RUNNER="${GOLD_PHASE:-$HOME/scripts/prism/gold-phase}/run_imp_nexusall.sh"
    [[ -f "$GOLD_RUNNER" ]] || { echo "[ERROR] Missing Gold runner: $GOLD_RUNNER" >&2; exit 2; }
    case "$JMP_TASK" in
        full|all) exec bash "$GOLD_RUNNER" "$WINDOW_FROM" "$WINDOW_TO" "$ENTITY" --brands "$BRAND" ;;
        load|loadstg) exec bash "$GOLD_RUNNER" "$WINDOW_FROM" "$WINDOW_TO" "$ENTITY" --brands "$BRAND" --jmp-task loadstg ;;
        dqstgprd|loadstgprd|posteval) exec bash "$GOLD_RUNNER" "$WINDOW_FROM" "$WINDOW_TO" "$ENTITY" --brands "$BRAND" --jmp-task "$JMP_TASK" ;;
        *) echo "[ERROR] Unsupported Gold-only D2S mode: $JMP_TASK" >&2; exit 2 ;;
    esac
fi

# The existing loaders and DQ use a 06:00-to-06:00 daily business window.
for dt in "$WINDOW_FROM" "$WINDOW_TO"; do
    [[ "$dt" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] &&
        [[ "$(date -d "$dt" +%F 2>/dev/null)" == "$dt" ]] || {
        echo "[ERROR] Invalid date: $dt (expected YYYY-MM-DD)" >&2
        exit 2
    }
done
[[ "$WINDOW_FROM" < "$WINDOW_TO" ]] || {
    echo '[ERROR] End date must be later than start date' >&2
    exit 2
}

case "$JMP_TASK" in
    full|all)
        export SKIP_LOADER=false SKIP_DQ=false SKIP_PROD_IMPORT=false
        ;;
    extract)
        export SKIP_LOADER=true SKIP_DQ=true SKIP_PROD_IMPORT=true
        ;; 
    load)
        export SKIP_LOADER=false SKIP_DQ=true SKIP_PROD_IMPORT=true
        ;;
    loaddq)
        export SKIP_LOADER=false SKIP_DQ=false SKIP_PROD_IMPORT=true
        ;;
    rawdq)
        export SKIP_LOADER=true SKIP_DQ=false SKIP_PROD_IMPORT=true
        ;;
    extractloaddq)
        export SKIP_LOADER=false SKIP_DQ=false SKIP_PROD_IMPORT=true
        ;;
    *)
        echo "[ERROR] Invalid --jmp-task: $JMP_TASK" >&2
        exit 2
        ;;
esac

printf '%s | INFO | PRISM mode     : %s\n' "$(date '+%F %T')" "$JMP_TASK"
printf '%s | INFO | Window from   : %s 06:00:00\n' "$(date '+%F %T')" "$WINDOW_FROM"
printf '%s | INFO | Window to     : %s 06:00:00\n' "$(date '+%F %T')" "$WINDOW_TO"

#if [[ "$JMP_TASK" == full ]]; then
if [[ "$JMP_TASK" == "full" || "$JMP_TASK" == "extract" || "$JMP_TASK" == "extractloaddq" ]]; then
    exec bash "$DIRECT2SILVER_PHASE/batch_daily_extract.sh" "$WINDOW_FROM" "$WINDOW_TO"
fi

# Resume stage: one day only because the existing loader and DQ operate on
# a daily file and share a transient loading table.
NEXT_DATE="$(date -d "$WINDOW_FROM +1 day" +%F)"
if [[ "$WINDOW_TO" != "$NEXT_DATE" ]]; then
    echo '[ERROR] Jump modes require exactly one 06:00-to-next-day-06:00 window' >&2
    exit 2
fi

# The existing manual batch_daily_loader.sh accepts two date arguments;
# it selects one archive in DMP_DIR/YYYYMM/YYYYMMDD/ and then follows
# the existing loader -> DQ handoff unless SKIP_DQ=true.
if [[ "$JMP_TASK" != rawdq ]]; then
    exec bash "$DIRECT2SILVER_PHASE/batch_daily_loader.sh" "$WINDOW_FROM" "$WINDOW_TO"
fi

# Raw DQ: call its existing five-argument interface directly.
LOAD_DATE="${WINDOW_FROM//-/}"
shopt -s nullglob
files=("$DMP_DIR/${LOAD_DATE:0:6}/$LOAD_DATE/${DB_SETTINGS_INPLAY1_FILE_PREFIX}_${LOAD_DATE}_exractdt_"*.jsonl.gz)
shopt -u nullglob
if (( ${#files[@]} != 1 )); then
    echo "[ERROR] Expected exactly one raw JSONL archive for $LOAD_DATE; found ${#files[@]}" >&2
    exit 2
fi
mkdir -p "$BASE_LOG_DIR"
STARTED_FILE="$BASE_LOG_DIR/dq_manual_${LOAD_DATE}.started"
STATUS_FILE="$BASE_LOG_DIR/dq_manual_${LOAD_DATE}.status"
exec bash "$DIRECT2SILVER_PHASE/batch_daily_dqraw.sh" \
    "${WINDOW_FROM} 06:00:00" "${WINDOW_TO} 06:00:00" "${files[0]}" \
    "$STARTED_FILE" "$STATUS_FILE"

