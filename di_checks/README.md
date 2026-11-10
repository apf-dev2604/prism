# PRISM InplayV1 Gap Watcher

Install `gap_watcher.py` and `run_gap_watcher.sh` in `/prism/di_checks/`.
The wrapper sources the existing PRISM `common/config/load_config.sh` and uses `shared_modules/etl_report.py`.
Run `01_gap_observations.sql` ONCE in the **NEXUS production DB** (config key `DB_SETTINGS_PRD_RAW_DB_NAME`). Grant the existing pipeline ETL user SELECT/INSERT/UPDATE on only this monitoring table, plus schema USAGE. Transaction tables are read-only.

Required exports from existing config: `DB_SETTINGS_INPLAY1_DB_HOST`, `_PORT`, `_USER`, `_NAME`, `_GAMETX_TABLENAME`; `DB_SETTINGS_PIPELINE_DB_HOST`, `_PORT`, `_PIPELINE_USER`; `DB_SETTINGS_DW_RAW_DB_NAME`, `_SCHEMA_NAME`, `_TABLENAME`; `DB_SETTINGS_PRD_RAW_DB_NAME`, `ETL_DB_USER_PWD`, `SHARED_MODS`. MySQL password is read from `GAP_MYSQL_PASSWORD`, `INPLAY1_DB_USER_PWD`, or `SRC_PWD` (first present). **Confirm the extractor's actual password export** before production use. Do not duplicate passwords in scripts.

SMTP: this package reads exported `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM`, `SMTP_TO`, `SMTP_STARTTLS` (also supports selected `SMTP_SETTINGS_*` aliases). **Compare with your actual `shared_modules/etl_alert.py`**: if that module uses different exported config names, map them in `smtp_send()` before enabling notification. The shared reporter is used directly; the shared alert module's `send_failure` API is failure-oriented and is not assumed to be a generic variance email sender.

Commands:

```bash
cd /home/allanf/scripts/prism/di_checks
bash -n run_gap_watcher.sh
python -m py_compile gap_watcher.py
bash ./run_gap_watcher.sh --check-only
bash ./run_gap_watcher.sh --as-of-date 2024-11-12 --lookback-days 7 --no-email
bash ./run_gap_watcher.sh --lookback-days 7
```

The last command checks the previous seven COMPLETED 06:00-06:00 business days. First observation for a matching three-way count is a silent PASS; later source increases and any three-way discrepancy are emailed. The `new_*_rows` numbers are **differences from the previous snapshot**, not proven new transaction IDs. A negative delta means a decrease; zero does not rule out replacements or altered financial data. No data is backfilled.

**Operational limits:** Each window runs one `COUNT(*)` range query on MySQL, Data Lake and Production. It uses no transaction row loading, but still scans appropriate index/table ranges and may be expensive without a MySQL `GameDate` index and PG date indexes. Counts are not a substitute for row-key, financial DQ, or ingestion watermark checks. Data from incomplete/failed ETL runs may trigger expected discrepancies; schedule after ETL+posteval has completed. Production query pruning requires partitioning on `enddatetime` and matching UTC-assumed boundaries.

**First run limitation:** 'Previous' means the previous watcher snapshot; existing Raw DQ and NEXUS DQ summary tables are not automatically assumed to represent a common three-system baseline. To populate historical 'previous' values, run the watcher once to establish its own baseline. Existing monitoring table `db_admin.etl_source_window_checks` remains untouched.

If monitoring SQL fails, the result is CHECK_ERROR and is emailed if SMTP is configured. An unrecoverable initial connection failure is logged and returns nonzero; it cannot necessarily be emailed if configuration/connection itself is broken. Configure an external cron failure notification for this case.
