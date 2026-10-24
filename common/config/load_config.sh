
#!/usr/bin/env bash
# ==============================================================
# PRISM - Global Configuration Loader
#
# Configuration:
#   common/config/config.toml  - Non-secret settings
#   common/.env                - Passwords and credentials
#
# Requires:
#   Bash 4.3+
#   Python 3.9+
#   Mike Farah yq v4 (TOML input support)
#
# Usage:
#   source "$HOME/scripts/prism/common/config/load_config.sh"
#
# All configuration variables are exported to child processes.
# ==============================================================

_prism_resolve_value() {

    local result="$1"
    local token variable replacement
    local iteration=0

    # Expand ~/ using the current user's home directory.
    if [[ "$result" == "~/"* ]]; then
        result="$HOME/${result:2}"
    fi

    # Resolve $NAME and ${NAME} references.
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
            echo "[PRISM CONFIG][ERROR] Unsupported reference: $result" >&2
            return 1
        fi

        if [[ ! -v $variable ]]; then
            echo "[PRISM CONFIG][ERROR] Undefined variable: $variable" >&2
            return 1
        fi

        replacement="${!variable}"

        result="${result//"$token"/"$replacement"}"
    done

    printf '%s' "$result"
}


_prism_load_config() {

    local config_dir config_file env_file
    local python_bin tmp_file
    local key value resolved
    local previous_allexport=0

    # ----------------------------------------------------------
    # 1. Locate configuration files
    #
    # Use the loader's own location so the project can move
    # to another Linux user's home directory.
    # ----------------------------------------------------------

    config_dir="$(
        cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P
    )" || return 1

    config_file="$config_dir/config.toml"
    env_file="$config_dir/../.env"

    # ----------------------------------------------------------
    # 2. Check prerequisites
    # ----------------------------------------------------------

    if ! command -v yq >/dev/null 2>&1; then
        echo "[PRISM CONFIG][ERROR] yq is not installed" >&2
        return 1
    fi

    if command -v python3.9 >/dev/null 2>&1; then
        python_bin="$(command -v python3.9)"
    elif command -v python3 >/dev/null 2>&1; then
        python_bin="$(command -v python3)"
    else
        echo "[PRISM CONFIG][ERROR] Python 3 is not installed" >&2
        return 1
    fi

    if [[ ! -r "$config_file" ]]; then
        echo "[PRISM CONFIG][ERROR] Missing or unreadable: $config_file" >&2
        return 1
    fi

    if [[ ! -r "$env_file" ]]; then
        echo "[PRISM CONFIG][ERROR] Missing or unreadable: $env_file" >&2
        return 1
    fi

    # ----------------------------------------------------------
    # 3. Parse TOML and flatten configuration names
    #
    # Unprefixed:
    #   COMMON_PATHS.BASE_DIR -> BASE_DIR
    #   PIPELINE_DIR.DMP_DIR  -> DMP_DIR
    #   SMTP.SMTP_HOST        -> SMTP_HOST
    #
    # Prefixed:
    #   DB_SETTINGS.INPLAY1.DB_HOST
    #       -> DB_SETTINGS_INPLAY1_DB_HOST
    #
    #   COMMON_ETL_SETTINGS.FETCH_SIZE
    #       -> COMMON_ETL_SETTINGS_FETCH_SIZE
    #
    # String arrays become comma-separated values.
    # Booleans become "true" or "false".
    #
    # Output uses NUL delimiters to preserve spacing.
    # ----------------------------------------------------------

    tmp_file="$(mktemp)" || return 1

    if ! "$python_bin" - "$config_file" "$tmp_file" <<'PY'
import json
import re
import subprocess
import sys

config_file = sys.argv[1]
output_file = sys.argv[2]

result = subprocess.run(
    ["yq", "-p=toml", "-o=json", ".", config_file],
    capture_output=True,
    text=True
)

if result.returncode != 0:
    raise RuntimeError(
        "TOML parsing failed: " + result.stderr.strip()
    )

config = json.loads(result.stdout)

UNPREFIXED_SECTIONS = {
    "COMMON_PATHS",
    "PIPELINE_DIR",
    "SMTP",
}

entries = []
seen = set()

def walk(parts, value):
    if isinstance(value, dict):
        for key, child in value.items():
            walk(parts + [key], child)
        return

    if parts[0] in UNPREFIXED_SECTIONS:
        name_parts = parts[1:]
    else:
        name_parts = parts

    name = "_".join(name_parts)

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("Invalid variable name: " + name)

    if name in seen:
        raise ValueError("Duplicate exported variable: " + name)

    seen.add(name)

    if isinstance(value, bool):
        text = "true" if value else "false"

    elif isinstance(value, list):
        if not all(isinstance(x, str) for x in value):
            raise ValueError(
                "Only arrays of strings are supported: " + name
            )
        if any("," in x for x in value):
            raise ValueError(
                "Array item contains delimiter comma: " + name
            )
        text = ",".join(value)

    elif isinstance(value, (str, int, float)):
        text = str(value)

    else:
        raise ValueError(
            "Unsupported TOML value for " + name
        )

    if "\x00" in text or "\n" in text:
        raise ValueError(
            "Multiline or NUL value not supported: " + name
        )

    entries.append((name, text))

for section, values in config.items():
    walk([section], values)

with open(output_file, "wb") as out:
    for name, value in entries:
        out.write(name.encode("utf-8") + b"\0")
        out.write(value.encode("utf-8") + b"\0")
PY
    then
        echo "[PRISM CONFIG][ERROR] Unable to parse config.toml" >&2
        rm -f -- "$tmp_file"
        return 1
    fi

    # ----------------------------------------------------------
    # 4. Resolve references and export TOML variables
    # ----------------------------------------------------------

    while IFS= read -r -d '' key &&
          IFS= read -r -d '' value; do

        if [[ ! "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
            echo "[PRISM CONFIG][ERROR] Invalid variable: $key" >&2
            rm -f -- "$tmp_file"
            return 1
        fi

        if ! resolved="$(_prism_resolve_value "$value")"; then
            rm -f -- "$tmp_file"
            return 1
        fi

        printf -v "$key" '%s' "$resolved"
        export "$key"

    done < "$tmp_file"

    rm -f -- "$tmp_file"

    # ----------------------------------------------------------
    # 5. Load secret variables from .env
    #
    # .env must contain trusted Bash-compatible assignments.
    # Preserve the caller's existing export-all setting.
    # ----------------------------------------------------------

    if [[ $- == *a* ]]; then
        previous_allexport=1
    fi

    set -a

    if ! source "$env_file"; then

        if (( ! previous_allexport )); then
            set +a
        fi

        echo "[PRISM CONFIG][ERROR] Failed to load .env" >&2
        return 1
    fi

    if (( ! previous_allexport )); then
        set +a
    fi

    # ----------------------------------------------------------
    # 6. Validate mandatory credentials
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
    # 7. Validate essential directory settings
    # ----------------------------------------------------------

    local required

    for required in \
        BASE_DIR \
        COMMON_DIR \
        SHARED_MODS \
        CONF_DIR \
        DIRECT2SILVER_PHASE \
        GOLD_PHASE \
        BASE_LOG_DIR \
        DMP_DIR \
        EXTRACT_LOG \
        EXTRACT_LOCK_FILE; do

        if [[ -z "${!required:-}" ]]; then
            echo "[PRISM CONFIG][ERROR] Missing: $required" >&2
            return 1
        fi

    done

    # Verify that config.toml describes this project.
    local actual_root

    actual_root="$(cd "$config_dir/../.." && pwd -P)" || return 1

    if [[ "$BASE_DIR" != "$actual_root" ]]; then
        echo "[PRISM CONFIG][ERROR] BASE_DIR does not match project root" >&2
        echo "[PRISM CONFIG] Configured: $BASE_DIR" >&2
        echo "[PRISM CONFIG] Actual:     $actual_root" >&2
        return 1
    fi

    # ----------------------------------------------------------
    # 8. Successful configuration load
    # ----------------------------------------------------------

    echo "[PRISM CONFIG][PASS] Global configuration loaded"
    echo "[PRISM CONFIG] BASE_DIR: $BASE_DIR"
    echo "[PRISM CONFIG] CONFIG: $config_file"

    return 0
}

_prism_load_config

