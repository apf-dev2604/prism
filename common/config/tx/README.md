# PRISM shared configuration (v2)

The global `etl.conf` uses Bash `KEY=value` assignments. Definitions are ordered so a parent directory (for example `BASE_DIR`) appears before child paths (`COMMON_DIR=$BASE_DIR/common`). One shared **Bash** loader handles these references and exports them for Bash and Python 3.9. Python scripts read `os.environ` and do **not** need a second parsing library or Python config loader when launched from a configured entry point.

## Installation

Copy `etl.conf` and `load_config.sh` to `~/scripts/nexus_etl_prod/common/config/`. Keep sensitive credentials only in `~/scripts/nexus_etl_prod/common/.env`, which must be trusted, Bash-compatible `KEY=value` syntax and have permissions `chmod 600`. Do not commit it to Git.

## Once per independent process chain

At the top of an independent Bash entry point, before starting any Python or child Bash:

```bash
source "$HOME/scripts/nexus_etl_prod/common/config/load_config.sh" || exit 1
python3.9 -c 'import os; print(os.environ["EXTRACT_LOCK_FILE"])'
```

From Bash: `echo "$EXTRACT_LOCK_FILE"`. From Python: `os.environ["EXTRACT_LOCK_FILE"]`, or `Path(os.environ["DMP_DIR"])`. Exported variables are inherited by normal and detached children (`subprocess.run`, `Popen`, `nohup`, `&`) **provided the child retains the inherited environment**. They survive the parent exiting while the child runs. They do not automatically appear in a fresh SSH session, an independently launched cron task, a new systemd unit, or an unrelated Bash/Python process. Each such entry point must source the same loader once.

Use `source`, not `bash load_config.sh`, since executing the loader as an independent process cannot export variables back to the caller. Avoid `sudo` or custom `env={...}` that removes required variables.

## Contract

- File naming intentionally retains `etl.conf`; Python consumers use `os.environ`.
- `~/` resolves against the account invoking the loader; a service account must have its own deployment path or override the loader location accordingly.
- `EXTRACT_LOCK_FILE=$BASE_LOG_DIR/extract_egms_gamestx.lock` puts the lock in `logs/`, matching the original layout. To place it inside `extract_logs/`, change it to `$EXTRACT_LOG/extract_egms_gamestx.lock` in **both** `etl.conf` and loader normalization if normalization remains enabled.
- Do not use arbitrary shell commands in `etl.conf` or `.env`; sourcing executes shell syntax. Protect both files from untrusted writes.
- Exporting secrets into process environments has security implications: restrict service-account permissions and do not log environments.
- Loading config is not sufficient by itself if older scripts still use hardcoded paths; scripts must reference these exported variables.
