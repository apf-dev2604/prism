-- PostgreSQL. Run during a DBA-approved maintenance window.
-- Fail closed if the source/target columns differ from the expected state.
-- No table data is deleted. Existing indexes/dependencies follow a column rename.
BEGIN;
DO $$
DECLARE ns text; oldcol text; old_count int; new_count int;
BEGIN
  FOREACH ns IN ARRAY ARRAY['etl_stage','public'] LOOP
    SELECT count(*), min(column_name)
      INTO old_count, oldcol
    FROM information_schema.columns
    WHERE table_schema=ns AND table_name='gametransaction_prod'
      AND column_name IN ('batchcode','batch_code');
    SELECT count(*) INTO new_count
    FROM information_schema.columns
    WHERE table_schema=ns AND table_name='gametransaction_prod'
      AND column_name='branchcode';
    IF old_count=1 AND new_count=0 THEN
      EXECUTE format('ALTER TABLE %I.%I RENAME COLUMN %I TO %I',
                     ns,'gametransaction_prod',oldcol,'branchcode');
    ELSIF old_count=0 AND new_count=1 THEN
      RAISE NOTICE '%.gametransaction_prod already has branchcode; unchanged',ns;
    ELSE
      RAISE EXCEPTION 'Ambiguous/missing columns on %.gametransaction_prod: old=%, new=%; no change', ns,old_count,new_count;
    END IF;
  END LOOP;
END $$;
COMMIT;

-- Validate after commit:
SELECT table_schema,table_name,column_name,data_type
FROM information_schema.columns
WHERE table_schema IN ('etl_stage','public')
  AND table_name='gametransaction_prod' AND column_name IN ('branchcode','goldphase');
