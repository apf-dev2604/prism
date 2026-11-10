#!/usr/bin/env bash
# Backward-compatible D2S entrypoint; the Gold Phase implementation is shared
# across InplayV1, InplayV2, and configured brands.
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$ROOT/run_imp_nexusall.sh" "$@"
