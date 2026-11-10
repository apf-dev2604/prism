-- INSTALL ONLY for future retention planning; intentionally NO EXECUTE grant to maintenance login.
-- The function does NOT check backups; approval must require backup+checksum+restore testing.
BEGIN;
CREATE FUNCTION public.retire_gametx_partition(p_partition text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET TimeZone = 'UTC'
AS $fn$
DECLARE
 v_parent oid := 'public.gametransaction_prod'::regclass;
 v_child oid; v_total integer; v_rank integer; v_month date; v_bound text;
 v_expected_start text; v_expected_end text;
BEGIN
 IF p_partition !~ '^gametransaction_prod_y[0-9]{4}m(0[1-9]|1[0-2])$' THEN
   RAISE EXCEPTION 'Unexpected partition name';
 END IF;
 -- Serialize with approved partition creation; locking parent also protects final eligibility.
 PERFORM pg_advisory_xact_lock(2024,1101);
 LOCK TABLE public.gametransaction_prod IN ACCESS EXCLUSIVE MODE;
 v_child := to_regclass(format('public.%I', p_partition));
 IF v_child IS NULL OR NOT EXISTS(SELECT 1 FROM pg_inherits
    WHERE inhparent=v_parent AND inhrelid=v_child) THEN
   RAISE EXCEPTION 'Target is not attached to approved parent';
 END IF;
 SELECT count(*) INTO v_total FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
 WHERE i.inhparent=v_parent AND c.relname ~ '^gametransaction_prod_y[0-9]{4}m(0[1-9]|1[0-2])$';
 IF v_total <= 7 THEN RETURN '[RETENTION SKIP] Seven or fewer partitions'; END IF;
 SELECT rank_pos INTO v_rank FROM (SELECT c.relname, row_number() OVER (ORDER BY c.relname) AS rank_pos
  FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
  WHERE i.inhparent=v_parent AND c.relname ~ '^gametransaction_prod_y[0-9]{4}m(0[1-9]|1[0-2])$') x
  WHERE x.relname=p_partition;
 IF v_rank IS NULL OR v_rank > v_total - 7 THEN RETURN '[RETENTION SKIP] Seven newest protected'; END IF;
 -- Require the seven newest partitions to be consecutive months.
 IF EXISTS (
   WITH m AS (
     SELECT make_date(substring(c.relname from 'y([0-9]{4})m')::int,
                      substring(c.relname from 'm([0-9]{2})$')::int, 1) AS mon
     FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
     WHERE i.inhparent=v_parent AND c.relname ~ '^gametransaction_prod_y[0-9]{4}m(0[1-9]|1[0-2])$'
   ), ranked AS (
     SELECT mon, row_number() OVER (ORDER BY mon DESC) AS rn,
            max(mon) OVER () AS latest FROM m
   )
   SELECT 1 FROM ranked
   WHERE rn <= 7 AND mon <> (latest - (rn - 1) * interval '1 month')::date
 ) THEN
   RAISE EXCEPTION 'Seven newest months are not consecutive; manual review required';
 END IF;
 v_month := make_date(substring(p_partition from 'y([0-9]{4})m')::int,
                       substring(p_partition from 'm([0-9]{2})$')::int,1);
 IF (v_month + interval '1 month') >= date_trunc('month', current_timestamp AT TIME ZONE 'UTC') THEN
   RETURN '[RETENTION SKIP] Current/future partition';
 END IF;
 SELECT pg_get_expr(c.relpartbound,c.oid) INTO v_bound FROM pg_class c WHERE c.oid=v_child;
 v_expected_start := to_char(v_month::timestamp + interval '6 hour','YYYY-MM-DD HH24:MI:SS');
 v_expected_end := to_char((v_month + interval '1 month')::timestamp + interval '6 hour','YYYY-MM-DD HH24:MI:SS');
 IF v_bound IS NULL OR position(v_expected_start IN v_bound)=0 OR position(v_expected_end IN v_bound)=0 THEN
  RAISE EXCEPTION 'Unexpected partition bounds: %',v_bound;
 END IF;
 EXECUTE format('ALTER TABLE public.gametransaction_prod DETACH PARTITION public.%I',p_partition);
 EXECUTE format('DROP TABLE public.%I',p_partition);
 RETURN '[PARTITION RETIRED] '||p_partition;
END;
$fn$;
ALTER FUNCTION public.retire_gametx_partition(text) OWNER TO nexus_partition_owner;
REVOKE ALL ON FUNCTION public.retire_gametx_partition(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.retire_gametx_partition(text) FROM dba_partition_maint;
COMMIT;

