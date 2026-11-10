-- READ ONLY. Confirm physical table/column spelling and index definitions.
SELECT table_schema,table_name,column_name,data_type,is_nullable,column_default
FROM information_schema.columns
WHERE table_schema IN ('etl_stage','public')
  AND lower(table_name)='gametransaction_prod'
  AND lower(column_name) IN ('batchcode','batch_code','branchcode','brachcode','goldphase','fulletl','startdatetime','enddatetime')
ORDER BY table_schema,column_name;

SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname IN ('etl_stage','public')
  AND lower(tablename)='gametransaction_prod'
ORDER BY schemaname,indexname;

SELECT table_schema,table_name,column_name,data_type
FROM information_schema.columns
WHERE table_name IN ('GameTransactionInplayV2','GameTransactionInplayV2_dmp')
  AND column_name='settled_datetime';
