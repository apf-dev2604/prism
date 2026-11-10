-- OPTIONAL / MANUAL ONLY: remove the obsolete branchcode column.
-- IMPORTANT: this drops the entire column, including V1 NOT_APPLICABLE values.
-- Run only after the large UPDATE has finished/committed and dependencies are checked.
-- ALTER TABLE needs ACCESS EXCLUSIVE; coordinate a maintenance window.
-- Review dependent views, materialized views, functions, dashboards, and indexes first.
-- Existing historical rename migration 02_rename_branchcode_and_fulletl.sql
-- must NOT be run after this migration.
BEGIN;
SET LOCAL lock_timeout = '5s';
ALTER TABLE etl_stage.gametransaction_prod DROP COLUMN IF EXISTS branchcode;
ALTER TABLE public.gametransaction_prod DROP COLUMN IF EXISTS branchcode;
COMMIT;
