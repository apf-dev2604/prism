# Gold Phase entrypoint (2026-11-04)

The standard D2S handoff uses `run_imp_nexusall.sh` directly. `run_nexus_imp.sh` is kept only as an existing backwards-compatible wrapper. Existing InplayV1 mapping and reporting are unchanged. InplayV2 gameTransaction uses `--time-pht-aware` for source PHT boundaries and normalizes `enddatetime` to the UTC-labelled 06:00-to-06:00 target reporting window once in its adapter.

InplayV2 DEV stage: `dev_dba_etl.gametransaction_stage_inplayv2`. DEV target: `dev_dba_prd.gametransaction_prod_inplayv2`. DEV DQ/audit: `dev_dba_etl`. InplayV1 retains its original environments.

Example: `bash run_imp_nexusall.sh 2026-05-26 2026-05-27 gameTransaction --brands inplayv2`

Source/target ID and financial totals must still be reconciled independently; the existing DEV DQ is batch-scoped. The external `common/config/config.toml` was not included in the source attachment and is not overwritten.
