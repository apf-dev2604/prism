-- ONE-TIME historical backfill, separate from schema migration.
-- Only change rows whose origin is explicitly identifiable. Review preflight counts.
-- Run only after confirming brand values in your database and the V1 route.
-- IMPORTANT: InplayV1 and InplayV2 may BOTH use the stored brand label 'Inplay'.
-- Never infer the workflow from brand alone. Cutover time must also be reviewed.
SELECT brand, count(*) AS total_rows, count(*) FILTER (WHERE fulletl IS NULL) AS not_classified,
       count(*) FILTER (WHERE fulletl IS TRUE) AS full_etl,
       count(*) FILTER (WHERE fulletl IS FALSE) AS gold_only
FROM public.gametransaction_prod GROUP BY brand ORDER BY brand;

-- OPTIONAL, MANUAL AFTER REVIEW: uncomment ONLY after verifying all rows under
-- the brand value represent the original InplayV1 full D2S-to-Gold workflow.
-- BEGIN;
-- UPDATE public.gametransaction_prod
-- SET fulletl = TRUE
-- WHERE brand = 'Inplay'
--   AND startdatetime < TIMESTAMPTZ '2026-05-26 06:00:00+00'
--   AND fulletl IS NULL;
-- This optional update requires confirmation that all rows before this boundary
-- were actually processed by V1, and that the V2 normalized timeline starts
-- at 2026-05-26 06:00:00+00. Do not include the boundary itself.
-- COMMIT;

-- For already known V2 rows, a separate explicit FALSE backfill can be approved
-- after verifying brand identity. Do not use blanket COALESCE or default TRUE.
