-- READ ONLY: no index changes are justified solely by renaming batchcode to branchcode.
-- Indexes that reference a renamed column remain bound to the same column.
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE (schemaname IN ('public','etl_stage') AND tablename='gametransaction_prod')
   OR (tablename='GameTransactionInplayV2' AND indexdef ILIKE '%settled_datetime%')
ORDER BY schemaname,tablename,indexname;
