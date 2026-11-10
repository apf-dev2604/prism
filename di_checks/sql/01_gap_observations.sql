-- Execute once on the NEXUS production/audit database defined by DB_SETTINGS_PRD_RAW_DB_NAME.
-- Table is a monitoring-only append log, not a transaction table.
CREATE SCHEMA IF NOT EXISTS db_admin;
CREATE TABLE IF NOT EXISTS db_admin.etl_gap_watch_observations (
    check_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    checked_at timestamptz NOT NULL DEFAULT now(),
    brand text NOT NULL,
    entity text NOT NULL,
    business_date date NOT NULL,
    window_from timestamp NOT NULL,
    window_to timestamp NOT NULL,
    previous_mysql_rows bigint,
    previous_datalake_rows bigint,
    previous_production_rows bigint,
    mysql_rows bigint,
    datalake_rows bigint,
    production_rows bigint,
    new_mysql_rows bigint,
    new_datalake_rows bigint,
    new_production_rows bigint,
    status text NOT NULL,
    alert_fingerprint text,
    notification_status text,
    notified_at timestamptz,
    notes text
);
CREATE INDEX IF NOT EXISTS idx_gap_watch_lookup
    ON db_admin.etl_gap_watch_observations
    (brand, entity, window_from, window_to, check_id DESC);
-- Execute the following grants as an authorized DBA, replacing role if needed:
-- GRANT USAGE ON SCHEMA db_admin TO your_pipeline_etl_role;
-- GRANT SELECT, INSERT, UPDATE ON db_admin.etl_gap_watch_observations TO your_pipeline_etl_role;
