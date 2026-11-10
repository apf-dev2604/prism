-- Execute as an authorized DBA; this script assumes parent table owner nexus_partition_owner.
-- PostgreSQL 13+; supported partition key types: timestamp and timestamptz.
-- Dates reflect PRISM UTC-assumed 06:00 business windows; timezone set explicitly.
BEGIN;
SET LOCAL TIME ZONE 'UTC';
CREATE FUNCTION public.ensure_gametx_partitions(p_from date, p_to date)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET TimeZone = 'UTC'
AS $function$
DECLARE
  parent_oid oid := 'public.gametransaction_prod'::regclass;
  part_type oid;
  month_cursor date;
  month_begin timestamp;
  month_end timestamp;
  window_end timestamp;
  child_name text;
  child_oid oid;
  actual_bound text;
  expected_lower text;
  expected_upper text;
BEGIN
  IF p_from IS NULL OR p_to IS NULL OR p_to <= p_from OR p_to > p_from + 366 THEN
    RAISE EXCEPTION 'Invalid window or exceeds 366 days';
  END IF;
  -- Mandatory safety: never execute as superuser.
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = current_user AND rolsuper) THEN
    RAISE EXCEPTION 'Partition definer must not be a superuser';
  END IF;
  SELECT a.atttypid INTO part_type FROM pg_attribute a
   WHERE a.attrelid=parent_oid AND a.attname='enddatetime' AND NOT a.attisdropped;
  IF part_type IS NULL OR part_type NOT IN ('pg_catalog.timestamp'::regtype::oid,
                                            'pg_catalog.timestamptz'::regtype::oid) THEN
    RAISE EXCEPTION 'enddatetime must be timestamp or timestamptz';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_partitioned_table p
                 JOIN pg_attribute a ON a.attrelid=p.partrelid
                 WHERE p.partrelid=parent_oid AND p.partstrat='r' AND p.partnatts=1
                   AND a.attname='enddatetime' AND p.partattrs[0]=a.attnum) THEN
    RAISE EXCEPTION 'Parent must be RANGE partitioned on enddatetime';
  END IF;
  PERFORM pg_advisory_xact_lock(2024,1101);
  month_cursor := date_trunc('month',p_from::timestamp)::date;
  window_end := p_to::timestamp + interval '6 hours';
  WHILE month_cursor::timestamp + interval '6 hours' < window_end LOOP
    month_begin := month_cursor::timestamp + interval '6 hours';
    month_end := (month_cursor + interval '1 month')::timestamp + interval '6 hours';
    child_name := 'gametransaction_prod_y' || to_char(month_cursor,'YYYY') || 'm' || to_char(month_cursor,'MM');
    child_oid := to_regclass(format('public.%I',child_name));
    IF child_oid IS NULL THEN
      IF part_type = 'pg_catalog.timestamptz'::regtype::oid THEN
        EXECUTE format('CREATE TABLE public.%I PARTITION OF public.gametransaction_prod FOR VALUES FROM (%L) TO (%L)',
         child_name, to_char(month_begin,'YYYY-MM-DD HH24:MI:SS')||'+00', to_char(month_end,'YYYY-MM-DD HH24:MI:SS')||'+00');
      ELSE
        EXECUTE format('CREATE TABLE public.%I PARTITION OF public.gametransaction_prod FOR VALUES FROM (%L) TO (%L)',
         child_name, to_char(month_begin,'YYYY-MM-DD HH24:MI:SS'),to_char(month_end,'YYYY-MM-DD HH24:MI:SS'));
      END IF;
      RAISE NOTICE '[PARTITION CREATED] %',child_name;
      child_oid := to_regclass(format('public.%I',child_name));
    ELSE
      RAISE NOTICE '[PARTITION EXISTS] %',child_name;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent=parent_oid AND inhrelid=child_oid) THEN
      RAISE EXCEPTION '% exists but is not attached to parent',child_name;
    END IF;
    SELECT pg_get_expr(c.relpartbound,c.oid) INTO actual_bound FROM pg_class c WHERE c.oid=child_oid;
    expected_lower := to_char(month_begin,'YYYY-MM-DD HH24:MI:SS');
    expected_upper := to_char(month_end,'YYYY-MM-DD HH24:MI:SS');
    IF actual_bound IS NULL OR position(expected_lower IN actual_bound)=0
       OR position(expected_upper IN actual_bound)=0 THEN
      RAISE EXCEPTION 'Unexpected bound for %: %',child_name,actual_bound;
    END IF;
    RAISE NOTICE '[PARTITION READY] %',child_name;
    month_cursor := (month_cursor + interval '1 month')::date;
  END LOOP;
END;
$function$;
ALTER FUNCTION public.ensure_gametx_partitions(date,date) OWNER TO nexus_partition_owner;
REVOKE ALL ON FUNCTION public.ensure_gametx_partitions(date,date) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ensure_gametx_partitions(date,date) TO dba_partition_maint;
COMMIT;

