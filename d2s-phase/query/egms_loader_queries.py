# !/usr/bin/env python3
"""Exact SQL statements extracted from the approved EGMS PostgreSQL loader.

Only SQL text was relocated; parameter binding and identifier escaping remain in the loader.
"""

# Original function: resolve_table_name; original line: 299
SQL_01_RESOLVE_TABLE_NAME = "\n            SELECT n.nspname, c.relname\n            FROM pg_catalog.pg_class c\n            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n            WHERE n.nspname = %s\n              AND c.relname = %s\n              AND c.relkind IN ('r', 'p')\n            LIMIT 1;\n            "

# Original function: resolve_table_name; original line: 316
SQL_02_RESOLVE_TABLE_NAME = "\n            SELECT n.nspname, c.relname\n            FROM pg_catalog.pg_class c\n            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n            WHERE n.nspname = %s\n              AND lower(c.relname) = lower(%s)\n              AND c.relkind IN ('r', 'p')\n            ORDER BY c.relname;\n            "

# Original function: resolve_table_name; original line: 339
SQL_03_RESOLVE_TABLE_NAME = "\n            SELECT n.nspname, c.relname\n            FROM pg_catalog.pg_class c\n            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n            WHERE lower(n.nspname) = lower(%s)\n              AND lower(c.relname) = lower(%s)\n              AND c.relkind IN ('r', 'p')\n            ORDER BY n.nspname, c.relname;\n            "

# Original function: resolve_table_name; original line: 362
SQL_04_RESOLVE_TABLE_NAME = "\n            SELECT n.nspname, c.relname\n            FROM pg_catalog.pg_class c\n            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n            WHERE lower(n.nspname) = lower(%s)\n              AND c.relkind IN ('r', 'p')\n            ORDER BY c.relname\n            LIMIT 20;\n            "

# Original function: assert_required_columns; original line: 397
SQL_05_ASSERT_REQUIRED_COLUMNS = '\n            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type\n            FROM pg_catalog.pg_attribute a\n            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid\n            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n            WHERE n.nspname = %s\n              AND c.relname = %s\n              AND a.attnum > 0\n              AND NOT a.attisdropped;\n            '

# Original function: assert_table_privileges; original line: 424
SQL_06_ASSERT_TABLE_PRIVILEGES = 'SELECT has_table_privilege(%s, %s, %s);'

# Original function: truncate_temp; original line: 468
SQL_07_TRUNCATE_TEMP = 'TRUNCATE TABLE {schema}.{table};'

# Original function: get_temp_count; original line: 479
SQL_08_GET_TEMP_COUNT = 'SELECT COUNT(*) FROM {schema}.{table};'

# Original function: validate_temp_values; original line: 489
SQL_09_VALIDATE_TEMP_VALUES = "\n        SELECT\n            COUNT(*) FILTER (\n                WHERE game_dt IS DISTINCT FROM\n                      NULLIF(data->>'GameDate', '')::timestamptz\n            ) AS date_mismatches,\n            COUNT(*) FILTER (\n                WHERE (data->>'JACKPOT_PAYOUT')::numeric IS DISTINCT FROM\n                    (COALESCE(NULLIF(data->>'JW1','')::numeric,0)\n                    +COALESCE(NULLIF(data->>'JW2','')::numeric,0)\n                    +COALESCE(NULLIF(data->>'JW3','')::numeric,0)\n                    +COALESCE(NULLIF(data->>'JW4','')::numeric,0)\n                    +COALESCE(NULLIF(data->>'JW5','')::numeric,0))\n            ) AS payout_mismatches,\n            COUNT(*) FILTER (\n                WHERE (data->>'JACKPOT_CONTRIBUTION')::numeric IS DISTINCT FROM\n                    (COALESCE(NULLIF(data->>'PC1','')::numeric,0)\n                    +COALESCE(NULLIF(data->>'PC2','')::numeric,0)\n                    +COALESCE(NULLIF(data->>'PC3','')::numeric,0)\n                    +COALESCE(NULLIF(data->>'PC4','')::numeric,0)\n                    +COALESCE(NULLIF(data->>'PC5','')::numeric,0))\n            ) AS contribution_mismatches\n        FROM {schema}.{table}\n    "

# Original function: validate_final_values; original line: 530
SQL_10_VALIDATE_FINAL_VALUES = '\n        SELECT COUNT(*)\n        FROM {ts}.{tt} t\n        LEFT JOIN {fs}.{ft} f ON f.id = t.id\n        WHERE f.id IS NULL\n           OR f.data IS DISTINCT FROM t.data\n           OR f.game_dt IS DISTINCT FROM t.game_dt\n    '

# Original function: get_temp_duplicate_id_count; original line: 557
SQL_11_GET_TEMP_DUPLICATE_ID_COUNT = '\n            SELECT COUNT(*)\n            FROM (\n                SELECT id\n                FROM {schema}.{table}\n                GROUP BY id\n                HAVING COUNT(*) > 1\n            ) d;\n            '

# Original function: get_existing_in_final_count; original line: 576
SQL_12_GET_EXISTING_IN_FINAL_COUNT = '\n            SELECT COUNT(*)\n            FROM {temp_schema}.{temp_table} t\n            JOIN {target_schema}.{target_table} f\n              ON f.id = t.id;\n            '

# Original function: write_existing_in_final_skips; original line: 596
SQL_13_WRITE_EXISTING_IN_FINAL_SKIPS = '\n        SELECT\n            t.id,\n            t.game_dt,\n            now() AS loaded_time\n        FROM {temp_schema}.{temp_table} t\n        JOIN {target_schema}.{target_table} f\n          ON f.id = t.id\n        ORDER BY t.game_dt, t.id;\n    '

# Original function: copy_jsonl_to_temp; original line: 780
SQL_14_COPY_JSONL_TO_TEMP = '\n        COPY {schema}.{table} (id, data, game_dt)\n        FROM STDIN WITH (FORMAT csv, DELIMITER E\'\t\', QUOTE \'"\', ESCAPE \'"\')\n    '

# Original function: insert_to_final; original line: 852
SQL_15_INSERT_TO_FINAL = '\n                WITH src AS (\n                    SELECT t.id, t.data, t.game_dt\n                    FROM {temp_schema}.{temp_table} t\n                ), ins AS (\n                    INSERT INTO {target_schema}.{target_table} (id, data, game_dt)\n                    SELECT id, data, game_dt\n                    FROM src\n                    ON CONFLICT (id) DO NOTHING\n                    RETURNING id\n                )\n                SELECT\n                    (SELECT COUNT(*) FROM ins) AS inserted_count,\n                    0 AS updated_count,\n                    (SELECT COUNT(*) FROM src) - (SELECT COUNT(*) FROM ins) AS skipped_count;\n                '

# Original function: insert_to_final; original line: 876
SQL_16_INSERT_TO_FINAL = "\n                WITH src AS (\n                    SELECT t.id, t.data, t.game_dt\n                    FROM {temp_schema}.{temp_table} t\n                ), before_match AS (\n                    SELECT\n                        s.id,\n                        CASE\n                            WHEN f.id IS NULL THEN 'insert'\n                            WHEN f.data IS DISTINCT FROM s.data\n                              OR f.game_dt IS DISTINCT FROM s.game_dt THEN 'update'\n                            ELSE 'skip'\n                        END AS action_type\n                    FROM src s\n                    LEFT JOIN {target_schema}.{target_table} f\n                      ON f.id = s.id\n                ), upserted AS (\n                    INSERT INTO {target_schema}.{target_table} (id, data, game_dt)\n                    SELECT id, data, game_dt\n                    FROM src\n                    ON CONFLICT (id)\n                    DO UPDATE SET\n                        data = EXCLUDED.data,\n                        game_dt = EXCLUDED.game_dt\n                    WHERE {target_schema}.{target_table}.data IS DISTINCT FROM EXCLUDED.data\n                       OR {target_schema}.{target_table}.game_dt IS DISTINCT FROM EXCLUDED.game_dt\n                    RETURNING id\n                )\n                SELECT\n                    COUNT(*) FILTER (WHERE action_type = 'insert') AS inserted_count,\n                    COUNT(*) FILTER (WHERE action_type = 'update') AS updated_count,\n                    COUNT(*) FILTER (WHERE action_type = 'skip') AS skipped_count\n                FROM before_match;\n                "

# Original function: copy_temp_to_history; original line: 935
SQL_17_COPY_TEMP_TO_HISTORY = '\n            WITH ins AS (\n                INSERT INTO {history_schema}.{history_table} (id, data, game_dt)\n                SELECT id, data, game_dt\n                FROM {temp_schema}.{temp_table}\n                ON CONFLICT (id) DO NOTHING\n                RETURNING id\n            )\n            SELECT COUNT(*) FROM ins;\n            '


