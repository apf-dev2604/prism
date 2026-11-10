#!/usr/bin/env bash
# Usage: bash ../shared_functions/run_with_alert.sh STAGE ENTITY FROM TO -- bash existing_job.sh [args]
set -Eeuo pipefail
if (( $# < 7 )) || [[ "${5:-}" != "--" ]]; then
  echo "Usage: $0 STAGE ENTITY FROM TO -- COMMAND [ARGS...]" >&2; exit 2
fi
stage="$1"; entity="$2"; from="$3"; to="$4"; shift 5
shared_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
log_dir="${ETL_ALERT_LOG_DIR:-/tmp/etl_alert_logs}"
mkdir -p "$log_dir"
log_file="$(mktemp "$log_dir/etl_XXXXXXXX.log")"
# Do not source .env in this wrapper; command caller or the job should load it.
# To supply SMTP values, source the existing project .env before this invocation.
set +e
"$@" > >(tee -a "$log_file") 2> >(tee -a "$log_file" >&2)
rc=$?
set -e
if (( rc != 0 )); then
  "${ETL_ALERT_PYTHON:-python3}" "$shared_dir/etl_alert.py" \
    --stage "$stage" --entity "$entity" --window-from "$from" \
    --window-to "$to" --exit-code "$rc" --error-log "$log_file" || true
fi
exit "$rc"

