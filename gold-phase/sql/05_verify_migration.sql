-- Read-only: expected fields in both staging and production.
SELECT table_schema,table_name,column_name,data_type,is_nullable,column_default
FROM information_schema.columns
WHERE table_schema IN ('etl_stage','public') AND lower(table_name)='gametransaction_prod'
AND column_name IN ('branchcode','batchcode','batch_code','goldphase','fulletl')
ORDER BY table_schema,table_name,column_name;
-- Confirm V2 indexed source timestamp, without transforming indexed column.
SELECT schemaname,tablename,indexname,indexdef FROM pg_indexes
WHERE tablename = 'GameTransactionInplayV2' AND indexdef ILIKE '%settled_datetime%';
