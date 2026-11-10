#!/usr/bin/env bash
# PRISM common configuration loader. Source in each INDEPENDENT entry-point Bash.
# It exports settings for Bash/Python children, including detached descendants.
# Example: source "$HOME/scripts/nexus_etl_prod/common/config/load_config.sh"
# NEVER use set -x when sourcing credentials. Source only trusted files.

if [[ -z "${BASH_VERSION:-}" ]]; then
  printf '[PRISM CONFIG ERROR] Bash is required\n' >&2
  return 1 2>/dev/null || exit 1
fi

_prism_conf="${PRISM_CONF:-$HOME/scripts/nexus_etl_prod/common/config/etl.conf}"
if [[ ! -f "$_prism_conf" || ! -r "$_prism_conf" ]]; then
  printf '[PRISM CONFIG ERROR] Cannot read etl.conf: %s\n' "$_prism_conf" >&2
  return 1 2>/dev/null || exit 1
fi

# Both files are Bash-compatible and sourced in order, while auto-export is enabled.
# This means $BASE_DIR is expanded before dependent $COMMON_DIR, etc.
# Avoid leaving the caller with altered 'allexport' if it was already set.
_prism_allexport_was_on=0

case $- in *a*) _prism_allexport_was_on=1 ;; esac
set -a
# shellcheck disable=SC1090
source "$_prism_conf"

_prism_status=$?
if (( _prism_status == 0 )); then
  # Expand ~/ once; dependent paths are derived from expanded BASE_DIR.
  # Bash typically expands a leading ~ in assignments, but normalize for safety.
  
  case "$BASE_DIR" in
    '~/'*) BASE_DIR="$HOME/${BASE_DIR#\~/}" ;;
  esac
  
  # Rebuild directory paths from the normalized base, independent of shell details.
  COMMON_DIR="$BASE_DIR/common"
  SHARED_MODS="$COMMON_DIR/shared_modules"
  CONF_DIR="$COMMON_DIR/config"
  ETL_CONF_FILE="$CONF_DIR/etl.conf"
  ENV_FILE="$COMMON_DIR/.env"
  DIRECT2SILVER_PHASE="$BASE_DIR/d2s"
  GOLD_PHASE="$BASE_DIR/gold_phase"
  BASE_LOG_DIR="$DIRECT2SILVER_PHASE/logs"
  DMP_DIR="$DIRECT2SILVER_PHASE/daily_dmp/jsonl"
  EXTRACT_LOG="$BASE_LOG_DIR/extract_logs"
  EXTRACT_LOCK_FILE="$BASE_LOG_DIR/extract_egms_gamestx.lock"
  if [[ ! -f "$ENV_FILE" || ! -r "$ENV_FILE" ]]; then
    printf '[PRISM CONFIG ERROR] Cannot read secrets file: %s\n' "$ENV_FILE" >&2
    _prism_status=1
  else
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    _prism_status=$?
  fi
fi
if (( !_prism_allexport_was_on )); then set +a; fi
unset _prism_conf _prism_allexport_was_on
if (( _prism_status != 0 )); then
  unset _prism_status
  return 1 2>/dev/null || exit 1
fi

unset _prism_status
