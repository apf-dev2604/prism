# Stored identifiers only — 2026-11-04

- CLI routing remains `--brands inplayv1` or `--brands inplayv2` (the V2 adapter retains internal key `inplay`).
- `public.gametransaction_prod.brand` and staging `etl_stage.gametransaction_prod.brand`: `inplay` for V1/V2.
- `db_admin.etl_dq_run_summary` and game DQ mismatch rows: `brand=inplay`, `entity=gameTransaction` for V1/V2.
- InplayV1 D2S `MySQL-DLAKE` audit writes `brand=inplay`, `entity=gameTransaction`; Gold upstream gate checks matching values.
- Strict delivery-control routing keys intentionally remain adapter-specific (`inplayv1` for V1, `inplay` internally for V2). They prevent accidentally changing existing release-control uniqueness and processing flow.
- DQ audit readers in strict pipeline, publisher and post-release verification match the new shared business labels, retaining exact `run_id` lookup for publish/verification.
- No extraction, amount calculations, partitions, business-window, DQ algorithms, phase gates, or logging behavior changed.

## Deployment note

This release does not change historical DB rows. If changing historical records manually, make sure matching DQ summary rows, DQ mismatch rows, and the D2S `MySQL-DLAKE` audit labels are consistent with their readers. Do not rename strict delivery-control brand keys without also changing routing and uniqueness; those are intentionally separate from the business brand. Existing production/staging rows labelled `Inplay` will not automatically be converted to `inplay`. Avoid indiscriminate updates to partitioned production or append-only financial audit without a controlled migration.
