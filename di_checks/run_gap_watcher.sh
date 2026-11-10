#!/usr/bin/env bash
set -Eeuo pipefail
source "$HOME/scripts/prism/common/config/load_config.sh" || exit 1
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${VENV_DIR:-$HOME/scripts/.venv}/bin/python"
"$PYTHON" "$ROOT/gap_watcher.py" "$@"
