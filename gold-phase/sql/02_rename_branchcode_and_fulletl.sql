-- PostgreSQL: review schema first using 01_inspect_existing_schema.sql.
-- Maintenance window required: ALTER TABLE RENAME acquires ACCESS EXCLUSIVE.
-- No data deletion and no uniqueness/index removal. Do NOT run while live importer is active.
BEGIN;
DO $$
DECLARE ns text; oldcol text; n_old integer; n_new integer;
BEGIN
  FOREACH ns IN ARRAY ARRAY['etl_stage','public'] LOOP
    IF to_regclass(format('%I.%I',ns,'gametransaction_prod')) IS NULL THEN
      RAISE EXCEPTION 'Missing expected table %.gametransaction_prod',ns;
    END IF;
    SELECT count(*),min(column_name) INTO n_old,oldcol FROM information_schema.columns
      WHERE table_schema=ns AND table_name='gametransaction_prod' AND column_name IN ('batchcode','batch_code');
    SELECT count(*) INTO n_new FROM information_schema.columns
      WHERE table_schema=ns AND table_name='gametransaction_prod' AND column_name='branchcode';
    IF n_old=1 AND n_new=0 THEN
      EXECUTE format('ALTER TABLE %I.%I RENAME COLUMN %I TO %I',ns,'gametransaction_prod',oldcol,'branchcode');
    ELSIF n_old=0 AND n_new=1 THEN
      RAISE NOTICE '%.gametransaction_prod.branchcode already correct',ns;
    ELSE
      RAISE EXCEPTION 'Ambiguous branch column on %.gametransaction_prod: old=%, branchcode=%',ns,n_old,n_new;
    END IF;
    SELECT count(*) INTO n_old FROM information_schema.columns
      WHERE table_schema=ns AND table_name='gametransaction_prod' AND column_name='goldphase';
    SELECT count(*) INTO n_new FROM information_schema.columns
      WHERE table_schema=ns AND table_name='gametransaction_prod' AND column_name='fulletl';
    IF n_old=1 AND n_new=0 THEN
      EXECUTE format('ALTER TABLE %I.%I RENAME COLUMN goldphase TO fulletl',ns,'gametransaction_prod');
    ELSIF n_old=0 AND n_new=1 THEN
      RAISE NOTICE '%.gametransaction_prod.fulletl already correct',ns;
    ELSIF n_old=0 AND n_new=0 THEN
      EXECUTE format('ALTER TABLE %I.%I ADD COLUMN fulletl boolean',ns,'gametransaction_prod');
    ELSE
      RAISE EXCEPTION 'Both goldphase and fulletl exist on %.gametransaction_prod. Inspect values before proceeding.',ns;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=ns AND table_name='gametransaction_prod' AND column_name='fulletl' AND data_type <> 'boolean') THEN
      RAISE EXCEPTION '%.gametransaction_prod.fulletl must be BOOLEAN',ns;
    END IF;
  END LOOP;
END $$;
COMMIT;
