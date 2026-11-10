#!/usr/bin/env bash
# PRISM: read-only verification after successful STRICT RELEASED.
# Usage: run_post_etl_verification.sh YYYY-MM-DD YYYY-MM-DD gameTransaction
# Inherits PRISM settings exported by the existing load_config.sh / parent runner.
set -Eeuo pipefail

if (( $# != 4 )); then
    echo "Usage: $0 YYYY-MM-DD YYYY-MM-DD gameTransaction" >&2
    exit 2
fi

DATE_FROM="$1"
DATE_TO="$2"
ENTITY="$3"
BRAND="$4"

if [[ "$ENTITY" != "gameTransaction" ]]; then
    echo "[POST RELEASE][ERROR] Unsupported entity for current verifier: $ENTITY" >&2
    exit 2
fi

: "${GOLD_PHASE:?GOLD_PHASE must already be exported by PRISM configuration}"
: "${SHARED_MODS:?SHARED_MODS must already be exported by PRISM configuration}"
: "${COMMON_ETL_SETTINGS_PHT_TIMEZONE:?PRISM timezone must already be exported}"

if [[ ! -f "$GOLD_PHASE/helpers/post_release_verify.py" ]]; then
    echo "[POST RELEASE][ERROR] Missing $GOLD_PHASE/helpers/post_release_verify.py" >&2
    exit 1
fi

# Use the same Python virtual environment that runs the existing PRISM pipeline.
PYTHON_BIN="${PRISM_PYTHON_BIN:-/home/allanf/scripts/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "[POST RELEASE][ERROR] Python executable unavailable: $PYTHON_BIN" >&2
    exit 1
fi

printf '[POST RELEASE] entity=%s from=%s to=%s action=verify-read-only\n' \
    "$ENTITY" "$DATE_FROM" "$DATE_TO"

cd "$GOLD_PHASE"
"$PYTHON_BIN" -m helpers.post_release_verify \
    --date-from "$DATE_FROM" \
    --date-to "$DATE_TO" \
    --entity "$ENTITY" \
    --brand "$BRAND"

printf '[POST RELEASE] verification complete; exit=0\n'

