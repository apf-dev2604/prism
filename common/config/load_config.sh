
#!/usr/bin/env bash
# ==============================================================
# PRISM - Global Configuration Loader
#
# Configuration:
#   common/config/config.toml  - Non-secret settings
#   common/.env                - Passwords and credentials
#
# Requires:
#   Bash 4+
#   Mike Farah yq v4 (with TOML input support)
#
# Usage:
#   source "$HOME/scripts/nexus_etl_prod/common/config/load_config.sh"
#
# All configuration variables are exported to child processes.
# ==============================================================

_prism_load_config() {

    local config_dir config_file env_file
    local key value resolved token name
    local line current

    config_dir="$HOME/scripts/prism/common/config"
    config_file="$config_dir/config.toml"
    env_file="$config_dir/../.env"

    # ----------------------------------------------------------
    # 1. Check prerequisites
    # ----------------------------------------------------------

    if ! command -v yq >/dev/null 2>&1; then
        echo "[PRISM CONFIG][ERROR] yq is not installed" >&2
        return 1
    fi

    if [[ ! -f "$config_file" ]]; then
        echo "[PRISM CONFIG][ERROR] Missing: $config_file" >&2
        return 1
    fi

    if [[ ! -f "$env_file" ]]; then
        echo "[PRISM CONFIG][ERROR] Missing: $env_file" >&2
        return 1
    fi

    # ----------------------------------------------------------
    # 2. Resolve references such as:
    #      $BASE_DIR/common
    #      ${BASE_LOG_DIR}/extract_logs
    #      ~/scripts/nexus_etl_prod
    #
    # References must point to previously exported variables.
    # ----------------------------------------------------------

    _prism_resolve_value() {

        local result="$1"
        local variable replacement
        local iteration=0

        # Expand ~/ using the current user's home directory.
        if [[ "$result" == "~/"* ]]; then
            result="$HOME/${result:2}"
        fi

        # Resolve supported $NAME and ${NAME} references.
        while [[ "$result" == *'$'* ]]; do

            ((iteration += 1))

            if (( iteration > 20 )); then
                echo "[PRISM CONFIG][ERROR] Too many variable expansions" >&2
                return 1
            fi

            if [[ "$result" =~ \$\{([a-zA-Z_][a-zA-Z0-9_]*)\} ]]; then

                token="${BASH_REMATCH[0]}"
                variable="${BASH_REMATCH[1]}"

            elif [[ "$result" =~ \$([a-zA-Z_][a-zA-Z0-9_]*) ]]; then

                token="${BASH_REMATCH[0]}"
                variable="${BASH_REMATCH[1]}"

            else
                echo "[PRISM CONFIG][ERROR] Unsupported variable reference: $result" >&2
                return 1
            fi

            if [[ ! -v "$variable" ]]; then
                echo "[PRISM CONFIG][ERROR] Undefined variable: $variable" >&2
                return 1
            fi

            replacement="${!variable}"

            result="${result//$token/$replacement}"
        done

        printf '%s' "$result"
    }

    # ----------------------------------------------------------
    # 3. Read TOML using yq and export configuration
    #
    # Example:
    #   [DB_SETTINGS.INPLAY1]
    #   GAMETX_TABLENAME = "EGMS_Game_Trans"
    #
    # Becomes:
    #   DB_SETTINGS_INPLAY1_GAMETX_TABLENAME
    # ----------------------------------------------------------

    local -a entries=()

    if ! mapfile -t entries < <(
        yq -p=toml -o=json '.' "$config_file" |
        yq -p=json -o=tsv \
            '.. | select(kind == "scalar") |
             [path | join("_"), .] | @tsv' -
    ); then
        echo "[PRISM CONFIG][ERROR] Failed to read TOML" >&2
        return 1
    fi

    for line in "${entries[@]}"; do

        key="${line%%$'\t'*}"
        value="${line#*$'\t'}"

        if [[ ! "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
            echo "[PRISM CONFIG][ERROR] Invalid variable: $key" >&2
            return 1
        fi

        if ! resolved="$(_prism_resolve_value "$value")"; then
            return 1
        fi

        printf -v "$key" '%s' "$resolved"
        export "$key"

    done

    # ----------------------------------------------------------
    # 4. Load secret variables from .env
    # ----------------------------------------------------------

    set -a

    if ! source "$env_file"; then
        set +a
        echo "[PRISM CONFIG][ERROR] Failed to load .env" >&2
        return 1
    fi

    set +a

    # ----------------------------------------------------------
    # 5. Validate mandatory secrets
    # ----------------------------------------------------------

    if [[ -z "${INPLAYV1_SRC_PWD:-}" ]]; then
        echo "[PRISM CONFIG][ERROR] INPLAYV1_SRC_PWD missing" >&2
        return 1
    fi

    if [[ -z "${DST_PWD:-}" ]]; then
        echo "[PRISM CONFIG][ERROR] DST_PWD missing" >&2
        return 1
    fi

    # ----------------------------------------------------------
    # 6. Verify resolved project directories
    # ----------------------------------------------------------

    if [[ -z "${BASE_DIR:-}" ||
          -z "${COMMON_DIR:-}" ||
          -z "${DIRECT2SILVER_PHASE:-}" ||
          -z "${GOLD_PHASE:-}" ]]; then

        echo "[PRISM CONFIG][ERROR] Required directories missing" >&2
        return 1
    fi

    echo "[PRISM CONFIG][PASS] Global configuration loaded"
    echo "[PRISM CONFIG] Base: $BASE_DIR"
    echo "[PRISM CONFIG] Config: $config_file"

    return 0
}

_prism_load_config

