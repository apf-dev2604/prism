#!/usr/bin/env python3
"""MySQL Artemis.EGMS_Game_Trans -> Data Lake DQ. Python 3.9.
Strict, disk-backed full comparison of the approved fields. Does not clean/modify data.
"""
import argparse
import csv
import json
import logging
import os
import smtplib
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo  # ADDED - Actual execution timestamps in Philippine time
from decimal import Decimal, InvalidOperation
from collections import OrderedDict
from email.message import EmailMessage
from pathlib import Path

import pymysql
import psycopg2
from psycopg2 import sql

# Shared modules use the configuration already exported by load_config.sh.
sys.path.insert(0, os.environ['SHARED_MODS'])
from etl_alert import send_failure
from etl_report import configure_logging, print_summary

# =============================================================================
# CHANGE VARIABLES HERE
# =============================================================================
#SKIP_PROD_IMPORT = True  # True: DQ only; False: PASS -> run_imp_nexusall.sh
SKIP_PROD_IMPORT = os.environ.get("SKIP_PROD_IMPORT", "false").lower() == "true"
# ADDED - Provisional business mapping: valid bet = TotalStakes (confirm source semantics).
VALID_BET_EQUALS_STAKES = True
BASE_DIR = Path(os.environ['DIRECT2SILVER_PHASE'])
IMPORT_BASH = Path(os.environ['GOLD_PHASE']) / 'run_imp_nexusall.sh'
MYSQL_HOST = os.environ['DB_SETTINGS_INPLAY1_DB_HOST']
MYSQL_PORT = int(os.environ['DB_SETTINGS_INPLAY1_DB_PORT'])
MYSQL_USER = os.environ['DB_SETTINGS_INPLAY1_DB_USER']
MYSQL_DB = os.environ['DB_SETTINGS_INPLAY1_DB_NAME']
MYSQL_TABLE = os.environ['DB_SETTINGS_INPLAY1_GAMETX_TABLENAME']
#PG_HOST = os.environ['DB_SETTINGS_DW_DB_HOST']
PG_HOST = os.environ['DB_SETTINGS_PIPELINE_DB_HOST']
PG_PORT = int(os.environ['DB_SETTINGS_DW_RAW_DB_PORT'])
#PG_PORT = os.environ['DB_SETTINGS.PIPELINE_DB_PORT']
ETL_USER = os.environ["DB_SETTINGS_PIPELINE_PIPELINE_USER"]
ETL_DB_USER_PWD = os.getenv("ETL_DB_USER_PWD", "")  # UPDATED - pass

PG_DB = os.environ['DB_SETTINGS_DW_RAW_DB_NAME']
# ADDED - Separate PostgreSQL DATABASE for required DQ audit persistence.
# Same RDS host, independent DB connection (not the Data Lake connection).


AUDIT_PG_HOST = os.environ['DB_SETTINGS_PIPELINE_DB_HOST']
AUDIT_PG_PORT = os.environ['DB_SETTINGS_PIPELINE_DB_PORT']
AUDIT_PG_DB = os.environ['DB_SETTINGS_DQ_DB_NAME']
PG_SCHEMA = os.environ['DB_SETTINGS_DW_RAW_SCHEMA_NAME']
PG_TABLENAME = os.environ['DB_SETTINGS_DW_RAW_DB_TABLENAME']
REPORT_DIR = BASE_DIR / 'work' / 'dq_reports'
BATCH_SIZE = 5000
MAX_DETAIL_ROWS = 10000
PHT = ZoneInfo(os.environ['COMMON_ETL_SETTINGS_PHT_TIMEZONE'])  # Display only; SQL unchanged


# ADDED - Descriptive, unique run_id matching existing DQ audit convention.
# Window values are the UTC-tagged wall-clock labels used by the current extractor.
def generate_dq_run_id(window_from, window_to):
    execution_timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')
    return ('dqrecon_mysql_dlake_inplayv1_game_'
            '%s+00:00_%s+00:00_%s' % (
                window_from.strftime('%Y-%m-%d %H:%M:%S'),
                window_to.strftime('%Y-%m-%d %H:%M:%S'),
                execution_timestamp))


def pht_now():
    return datetime.now(PHT)


def hhmmss(seconds):
    total = int(round(seconds))
    return '%02d:%02d:%02d' % (total // 3600, (total % 3600) // 60, total % 60)

NUMERIC_FIELDS = ('TotalStakes', 'TotalWins') + tuple('JW%d' % i for i in range(1, 6)) + tuple('PC%d' % i for i in range(1, 6))
DERIVED = ('JACKPOT_PAYOUT', 'JACKPOT_CONTRIBUTION')
SOURCE_FIELDS = ('TransactionID', 'GameDate') + NUMERIC_FIELDS
TARGET_FIELDS = ('TransactionID', 'GameDate') + NUMERIC_FIELDS + DERIVED


def notify_failure(reason):
    """Preserve existing failure call sites; delegate SMTP to PRISM shared alert."""
    try:
        send_failure(
            stage='MySQL-DLAKE-DQRAW', entity='gameTransaction',
            window_from=globals().get('ALERT_FROM_DATE', 'unknown'),
            window_to=globals().get('ALERT_TO_DATE', 'unknown'),
            exit_code=1, error_log=str(globals().get('ALERT_LOG_PATH', '')),
        )
    except Exception:
        logging.exception('Shared DQ failure email delivery failed')


def parse_day(value):
    return datetime.strptime(value, '%Y-%m-%d').date()


def parse_window(value):
    return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')


def normalize_date(value):
    """Compare source DATETIME and UTC-tagged target by *wall clock* per current extractor contract.
    This is NOT a timezone-correct instant comparison.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat(sep=' ', timespec='microseconds')
    raw = str(value).strip()
    if raw.endswith('Z'):
        raw = raw[:-1]
    try:
        return datetime.fromisoformat(raw).replace(tzinfo=None).isoformat(sep=' ', timespec='microseconds')
    except ValueError:
        raise ValueError('invalid GameDate: %r' % value)


def num(value):
    if value is None or value == '':
        return None
    try:
        n = Decimal(str(value).strip())
        if not n.is_finite():
            raise ValueError('non-finite decimal')
        return format(n.normalize(), 'f')
    except (InvalidOperation, ValueError, TypeError) as e:
        raise ValueError('invalid numeric %r: %s' % (value, e))


def computed(values):
    return format(sum((Decimal(num(v) or '0') for v in values), Decimal('0')).normalize(), 'f')


def canonical_source(row):
    obj = {'TransactionID': str(row['TransactionID']), 'GameDate': normalize_date(row['GameDate'])}
    for col in NUMERIC_FIELDS:
        obj[col] = num(row[col])
    obj['JACKPOT_PAYOUT'] = computed(row['JW%d' % i] for i in range(1, 6))
    obj['JACKPOT_CONTRIBUTION'] = computed(row['PC%d' % i] for i in range(1, 6))
    return obj


def canonical_target(row):
    pk, data, game_dt = row
    if not isinstance(data, dict):
        raise ValueError('target data is not JSON object')
    obj = {'TransactionID': str(data.get('TransactionID')), 'GameDate': normalize_date(data.get('GameDate'))}
    for col in NUMERIC_FIELDS + DERIVED:
        obj[col] = num(data.get(col))
    if obj['TransactionID'] != str(pk):
        raise ValueError('data.TransactionID does not equal id')
    if obj['GameDate'] != normalize_date(game_dt):
        raise ValueError('data.GameDate does not equal game_dt')
    if obj['JACKPOT_PAYOUT'] != computed(data.get('JW%d' % i) for i in range(1, 6)):
        raise ValueError('incorrect JACKPOT_PAYOUT')
    if obj['JACKPOT_CONTRIBUTION'] != computed(data.get('PC%d' % i) for i in range(1, 6)):
        raise ValueError('incorrect JACKPOT_CONTRIBUTION')
    return obj


def record_detail(writer, stats, row_type, transaction_id, column='', mysql='', pg=''):
    stats['discrepancies'] += 1
    stats[row_type] = stats.get(row_type, 0) + 1
    if stats['details_written'] < MAX_DETAIL_ROWS:
        writer.writerow([row_type, transaction_id, column, str(mysql)[:500], str(pg)[:500]])
        stats['details_written'] += 1


# APPROVED source MySQL -> destination PostgreSQL JSONB logical-type matrix.
# Strict financial rule: do not silently approve FLOAT/DOUBLE for money.
FINANCIAL_ALLOWED_MYSQL = ('decimal', 'numeric')
FIELD_TYPE_MATRIX = OrderedDict([
    ('TransactionID', {'mysql': ('varchar', 'char', 'text', 'tinytext', 'mediumtext', 'longtext', 'bigint', 'int', 'integer'), 'postgres': 'id TEXT and data.TransactionID string; exact identity'}),
    ('GameDate', {'mysql': ('datetime',), 'postgres': 'game_dt TIMESTAMPTZ UTC-tagged wall clock; data.GameDate string'}),
    *[(field, {'mysql': FINANCIAL_ALLOWED_MYSQL, 'postgres': 'data.%s exact NUMERIC-compatible string or number' % field})
       for field in NUMERIC_FIELDS],
    ('JACKPOT_PAYOUT', {'mysql': ('computed',), 'postgres': 'data.JACKPOT_PAYOUT exact NUMERIC'}),
    ('JACKPOT_CONTRIBUTION', {'mysql': ('computed',), 'postgres': 'data.JACKPOT_CONTRIBUTION exact NUMERIC'}),
])


def types_preflight(mysql_conn, pg_conn):
    with mysql_conn.cursor(pymysql.cursors.Cursor) as c:
        c.execute("""SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
                     FROM information_schema.COLUMNS
                     WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s""", (MYSQL_DB, MYSQL_TABLE))
        mysql_meta = {r[0]: {'type': r[1].lower(), 'precision': r[2], 'scale': r[3], 'nullable': r[4]} for r in c.fetchall()}
    errors = []
    for field, rule in FIELD_TYPE_MATRIX.items():
        if 'computed' in rule['mysql']:
            continue
        col = mysql_meta.get(field)
        if not col:
            errors.append('Missing MySQL column: ' + field)
        elif col['type'] not in rule['mysql']:
            errors.append('%s: MySQL type %s not allowed; accepted=%s' % (field, col['type'], rule['mysql']))
        elif field in NUMERIC_FIELDS and (col['precision'] is None or col['scale'] is None):
            errors.append('%s: MySQL precision/scale unavailable' % field)
    with pg_conn.cursor() as c:
        c.execute("""SELECT column_name, data_type FROM information_schema.columns
                     WHERE table_schema=%s AND table_name=%s""", (PG_SCHEMA, PG_TABLENAME))
        pg_types = dict(c.fetchall())
    accepted = {'id': ('text',), 'data': ('jsonb',), 'game_dt': ('timestamp with time zone',)}
    for col, allowed in accepted.items():
        if pg_types.get(col) not in allowed:
            errors.append('PG %s expected %s, found %r' % (col, allowed, pg_types.get(col)))
    if errors:
        raise RuntimeError('DATATYPE MATRIX FAIL: ' + '; '.join(errors))
    return {'mysql_column_metadata': mysql_meta, 'postgresql_column_types': pg_types,
            'approved_field_matrix': FIELD_TYPE_MATRIX,
            'time_mapping': 'MySQL GameDate DATETIME -> PG game_dt TIMESTAMPTZ; UTC-tagged wall-clock (no PHT shift)'}


# Independent indexed-range SQL aggregate checks; never use JSONB GameDate in WHERE.
# Query results are retrieved as Decimal values, never binary float.
AGGREGATE_FIELDS = ('transaction_count', 'TotalStakes', 'TotalWins',
                    'JACKPOT_PAYOUT', 'JACKPOT_CONTRIBUTION') + tuple('JW%d' % i for i in range(1, 6)) + tuple('PC%d' % i for i in range(1, 6))


def mysql_financial_aggregates(conn, from_dt, to_dt):
    parts = ['COUNT(*) AS transaction_count', 'COUNT(DISTINCT `TransactionID`) AS distinct_keys']
    for f in ('TotalStakes', 'TotalWins') + tuple('JW%d' % i for i in range(1,6)) + tuple('PC%d' % i for i in range(1,6)):
        parts.append('COALESCE(SUM(`%s`),0) AS `%s`' % (f, f))
    for name, prefix in (('JACKPOT_PAYOUT', 'JW'), ('JACKPOT_CONTRIBUTION', 'PC')):
        expr = ' + '.join('COALESCE(`%s%d`,0)' % (prefix, i) for i in range(1,6))
        parts.append('COALESCE(SUM(%s),0) AS `%s`' % (expr, name))
    query = 'SELECT ' + ', '.join(parts) + ' FROM `Artemis`.`EGMS_Game_Trans` WHERE `GameDate` >= %s AND `GameDate` < %s'
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query, (from_dt, to_dt))
        raw = cur.fetchone()
    return {k: Decimal(str(v)) for k,v in raw.items()}


def pg_financial_aggregates(conn, from_dt, to_dt):
    cols = ['COUNT(*) AS transaction_count', 'COUNT(DISTINCT id) AS distinct_keys']
    for f in AGGREGATE_FIELDS[1:]:
        # Identifiers here come exclusively from hardcoded allowlist, not user input.
        cols.append("COALESCE(SUM((data->>'%s')::numeric),0) AS \"%s\"" % (f,f))
    statement = sql.SQL('SELECT ' + ', '.join(cols) +
        " FROM {}.{} WHERE game_dt >= (%s::timestamp AT TIME ZONE 'UTC') "
        "AND game_dt < (%s::timestamp AT TIME ZONE 'UTC')").format(
            sql.Identifier(PG_SCHEMA), sql.Identifier(PG_TABLENAME))
    with conn.cursor() as cur:
        cur.execute(statement, (from_dt, to_dt))
        row = cur.fetchone()
    return {key: Decimal(str(val)) for key,val in zip(('transaction_count','distinct_keys') + AGGREGATE_FIELDS[1:],row)}


def reconcile_financial_aggregates(mysql_conn, pg_conn, from_dt, to_dt):
    source = mysql_financial_aggregates(mysql_conn, from_dt, to_dt)
    target = pg_financial_aggregates(pg_conn, from_dt, to_dt)
    comparisons = {}
    for key in ('transaction_count','distinct_keys') + AGGREGATE_FIELDS[1:]:
        comparisons[key] = {'mysql': str(source[key]), 'postgresql': str(target[key]),
                            'status': 'PASS' if source[key] == target[key] else 'FAIL'}
    return comparisons


# ADDED - Business-oriented field mapping, derived from existing independent SQL
# aggregates. These are NOT additional unverified calculations.
def build_audit_metrics(aggregates):
    source = {k: Decimal(v['mysql']) for k, v in aggregates.items()}
    target = {k: Decimal(v['postgresql']) for k, v in aggregates.items()}
    result = {}
    for side, values in (('source', source), ('target', target)):
        result[side + '_distinct_keys'] = int(values['distinct_keys'])
        result[side + '_duplicate_keys'] = int(values['transaction_count'] - values['distinct_keys'])
        result[side + '_total_bet'] = str(values['TotalStakes'])
        result[side + '_total_payout'] = str(values['TotalWins'])
        result[side + '_total_jackpot_contribution'] = str(values['JACKPOT_CONTRIBUTION'])
        result[side + '_total_jackpot_payout'] = str(values['JACKPOT_PAYOUT'])
    return result


def build_column_evidence(types, aggregates, stats):
    evidence = []
    for field, mapping in FIELD_TYPE_MATRIX.items():
        meta = types['mysql_column_metadata'].get(field, {})
        if field in ('TransactionID', 'GameDate'):
            check = 'record_by_record'
        elif field in aggregates:
            check = 'record_by_record_and_independent_sum'
        else:
            check = 'record_by_record_and_derived_arithmetic'
        evidence.append({
            'field': field,
            'mysql_type': meta.get('type', 'DERIVED'),
            'mysql_precision': meta.get('precision'),
            'mysql_scale': meta.get('scale'),
            'approved_mysql_types': list(mapping['mysql']),
            'postgresql_mapping': mapping['postgres'],
            'physical_postgresql_types': types['postgresql_column_types'],
            'validation': check,
            'aggregate_status': aggregates[field]['status'] if field in aggregates else None,
            'overall_row_validation_status': 'PASS' if stats['discrepancies'] == 0 else 'FAIL',
        })
    return evidence


def save_summary_pg(conn, summary):
    """REQUIRED: persist DQ result in iestdbrds.db_admin.etl_dq_run_summary.

    Never swallow errors or treat a JSON report as a replacement for DB audit.
    Existing table is unchanged: select only columns that physically exist.
    """
    permitted = {'run_id':summary['run_id'],  # ADDED - mandatory audit run identifier
                 'brand':'inplay','entity':'gameTransaction','etl_stage':'MySQL-DLAKE',
                 'window_from':summary['window_from'], 'window_to':summary['window_to'],
                 'status':summary['status'],
                 # UPDATED - stage-specific readiness in existing trust_result; status remains PASS/FAIL.
                 'trust_result':summary['mysql_dlake_trust_result'],
                 'source_rows':summary['source_rows'], 'target_rows':summary['target_rows'],
                 **summary['audit_metrics'],
                 'missing_in_target':summary['missing_in_target'],
                 'hash_mismatch_rows':summary['value_mismatches'],
                 'duration_seconds':summary['duration_seconds'],
                 'started_at_pht':summary['started_at_pht'],
                 'completed_at_pht':summary['completed_at_pht'],
                 'duration_hhmmss':summary['duration_hhmmss'],
                 'notes':json.dumps({'report': summary['detail_csv'], 'counts':summary['stats'],
                                     'audit_metrics':summary['audit_metrics'],
                                     'column_evidence_in_json': summary.get('summary_report_path'),
                                     'started_at_pht':summary['started_at_pht'],
                                     'completed_at_pht':summary['completed_at_pht'],
                                     'duration_hhmmss':summary['duration_hhmmss']})[:3000]}
    try:
        with conn.cursor() as c:
            c.execute("SELECT column_name FROM information_schema.columns "
                      "WHERE table_schema=%s AND table_name=%s",
                      ('db_admin', 'etl_dq_run_summary'))
            available = {r[0] for r in c.fetchall()}
            if not available:
                raise RuntimeError('Required db_admin.etl_dq_run_summary unavailable in '
                                   + AUDIT_PG_DB + ' (missing table or no metadata access)')
            required = {'run_id','brand','entity','window_from','window_to','status','source_rows',
                        'target_rows','duration_seconds'}
            missing = required - available
            if missing:
                raise RuntimeError('Required DQ audit columns unavailable: ' + ', '.join(sorted(missing)))
            if 'trust_result' not in available:
                raise RuntimeError('Required trust_result audit column unavailable in iestdbrds')
            required_metrics = set(summary['audit_metrics'])
            missing_metrics = required_metrics - available
            if missing_metrics:
                raise RuntimeError('Required audit metric columns unavailable: ' + ', '.join(sorted(missing_metrics)))
            fields = [f for f in permitted if f in available]
            statement = sql.SQL('INSERT INTO {}.{} ({}) VALUES ({})').format(
                sql.Identifier('db_admin'), sql.Identifier('etl_dq_run_summary'),
                sql.SQL(',').join(map(sql.Identifier, fields)),
                sql.SQL(',').join(sql.Placeholder() for _ in fields))
            c.execute(statement, [permitted[k] for k in fields])
            # ADDED - Retain matrix evidence for every governed column in an independent
            # detail table. Summary + all column evidence commit together atomically.
            c.execute('''SELECT to_regclass('db_admin.etl_dq_column_checks')''')
            if c.fetchone()[0] is None:
                raise RuntimeError('Missing required db_admin.etl_dq_column_checks; run supplied DDL in iestdbrds')
            for item in summary['column_level_evidence']:
                c.execute('''INSERT INTO db_admin.etl_dq_column_checks
                    (run_id, etl_stage, source_column, target_mapping,
                     source_mysql_type, source_numeric_precision, source_numeric_scale,
                     approved_mysql_types, target_physical_types, validation_method,
                     aggregate_status, row_validation_status, datatype_status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s)''',
                    (summary['run_id'], 'MySQL-DLAKE', item['field'],
                     item['postgresql_mapping'], item['mysql_type'],
                     item['mysql_precision'], item['mysql_scale'],
                     json.dumps(item['approved_mysql_types']),
                     json.dumps(item['physical_postgresql_types']),
                     item['validation'], item['aggregate_status'],
                     item['overall_row_validation_status'], 'PASS'))
        conn.commit()
        logging.info('DQ audit persisted to %s.db_admin.etl_dq_run_summary', AUDIT_PG_DB)
    except Exception:
        conn.rollback()
        raise


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--from', dest='from_dt', required=True)
    p.add_argument('--to', dest='to_dt', required=True)
    p.add_argument('--file', default=None, help='Optional exact extracted JSONL path for audit/log linkage')
    args = p.parse_args()
    start = parse_window(args.from_dt)
    stop = parse_window(args.to_dt)
    dq_run_id = generate_dq_run_id(start, stop)  # ADDED - generate once per DQ execution
    if start.hour != 6 or (start.minute, start.second) != (0, 0) or stop != start + timedelta(days=1):
        p.error('DQ requires exactly one business day: 06:00:00 to next day 06:00:00 (exclusive)')
    if args.file and not Path(args.file).is_file():
        p.error('Input archive missing: ' + args.file)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    report_prefix = 'mysql_dlake_%s_%s' % (start.strftime('%Y%m%d'), stamp)
    log_path = REPORT_DIR / (report_prefix + '.log')
    globals().update(ALERT_FROM_DATE=args.from_dt, ALERT_TO_DATE=args.to_dt, ALERT_LOG_PATH=log_path)
    configure_logging(log_path)
    # ADDED - Keep actual execution times separate from the business-day window.
    started_at_pht = pht_now()
    logging.info('Started at PHT : %s', started_at_pht.strftime('%Y-%m-%d %H:%M:%S'))
    logging.info('Phase          : MySQL -> Data Lake RAW DATA QUALITY')
    logging.info('Remote MySQL   : %s:%s', MYSQL_HOST, MYSQL_PORT)
    logging.info('Remote PG      : %s:%s', PG_HOST, PG_PORT)
    logging.info('Source         : %s.%s', MYSQL_DB, MYSQL_TABLE)
    logging.info('Data Lake      : %s.%s.%s', PG_DB, PG_SCHEMA, PG_TABLENAME)
    logging.info('Audit database : %s', AUDIT_PG_DB)
    logging.info('Window from UTC: %s', args.from_dt)
    logging.info('Window to UTC  : %s', args.to_dt)
    logging.info('Report dir     : %s', REPORT_DIR)
    t0 = time.monotonic()
    path = REPORT_DIR / (report_prefix + '_details.csv')
    summary_path = REPORT_DIR / (report_prefix + '_summary.json')
    stats = {'details_written': 0, 'discrepancies': 0, 'missing_in_target': 0, 'extra_in_target': 0,
             'value_mismatches': 0, 'mysql_duplicate_ids': 0, 'pg_duplicate_ids': 0,
             'invalid_mysql_values': 0, 'invalid_pg_values': 0}
    source_rows = 0
    target_rows = 0
    mysql = pg = audit_pg = None
    sqlite_path = None
    try:
        with tempfile.NamedTemporaryFile(prefix='egms_dq_', suffix='.sqlite', dir=str(REPORT_DIR), delete=False) as tmp:
            sqlite_path = Path(tmp.name)
        disk = sqlite3.connect(str(sqlite_path))
        disk.execute('PRAGMA journal_mode=OFF')
        disk.execute('PRAGMA synchronous=OFF')
        disk.execute('CREATE TABLE source (id TEXT PRIMARY KEY, payload TEXT NOT NULL, matched INTEGER NOT NULL DEFAULT 0)')
        mysql = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
                                password=os.environ['INPLAYV1_SRC_PWD'], database=MYSQL_DB,
                                charset='utf8mb4', cursorclass=pymysql.cursors.SSDictCursor,
                                connect_timeout=15, read_timeout=3600)
        pg = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=ETL_USER,
                              password=os.environ['ETL_DB_USER_PWD'], dbname=PG_DB,
                              sslmode='require', connect_timeout=15)
        # ADDED - Mandatory, distinct database connection for DQ audit. Fail before
        # multi-million-row reconciliation if the audit database is unreachable.
        audit_pg = psycopg2.connect(
            host=AUDIT_PG_HOST, port=AUDIT_PG_PORT, user=ETL_USER,
            password=os.getenv(ETL_DB_USER_PWD) or os.environ['ETL_DB_USER_PWD'],
            dbname=AUDIT_PG_DB, sslmode='require', connect_timeout=15)
        logging.info('DQ audit DB connected: db=%s host=%s', AUDIT_PG_DB, AUDIT_PG_HOST)
        types = types_preflight(mysql, pg)
        # ADDED: independent source/target SQL SUM and COUNT reconciliation.
        aggregates = reconcile_financial_aggregates(mysql, pg, args.from_dt, args.to_dt)
        logging.info('SQL aggregate check: %s', {k:v['status'] for k,v in aggregates.items()})
        with path.open('w', newline='', encoding='utf-8') as fout:
            writer = csv.writer(fout)
            writer.writerow(['issue', 'transaction_id', 'field', 'mysql_value', 'pg_value'])
            # Store full source window in a disk-backed key index, including duplicate checks.
            with mysql.cursor() as cur:
                cur.execute('SELECT ' + ','.join('`%s`' % c for c in SOURCE_FIELDS) +
                            ' FROM `Artemis`.`EGMS_Game_Trans` WHERE `GameDate` >= %s AND `GameDate` < %s',
                            (args.from_dt, args.to_dt))
                for row in cur:
                    source_rows += 1
                    tid = str(row['TransactionID'])
                    try:
                        obj = canonical_source(row)
                        disk.execute('INSERT INTO source (id,payload) VALUES (?,?)', (tid,json.dumps(obj)))
                    except sqlite3.IntegrityError:
                        record_detail(writer, stats, 'mysql_duplicate_ids', tid)
                    except Exception as exc:
                        record_detail(writer, stats, 'invalid_mysql_values', tid, 'validation', str(exc))
                    if source_rows % BATCH_SIZE == 0:
                        disk.commit()
                        logging.info('Source rows indexed %d', source_rows)
            disk.commit()
            # Strict check that rows belong to the *same* business window in PG.
            # UTC wall-clock boundaries intentionally match current source extractor convention.
            query = sql.SQL('''SELECT id, data, game_dt AT TIME ZONE 'UTC' FROM {}.{}
                             WHERE game_dt >= (%s::timestamp AT TIME ZONE 'UTC')
                               AND game_dt < (%s::timestamp AT TIME ZONE 'UTC')''').format(
                                sql.Identifier(PG_SCHEMA),sql.Identifier(PG_TABLENAME))
            cur = pg.cursor(name='egms_dq_rows_%d' % os.getpid())
            cur.itersize = BATCH_SIZE
            cur.execute(query, (args.from_dt,args.to_dt))
            for row in cur:
                target_rows += 1
                tid = str(row[0])
                try:
                    obj = canonical_target(row)
                except Exception as exc:
                    record_detail(writer, stats, 'invalid_pg_values', tid, 'validation', str(exc))
                    obj = None
                matched = disk.execute('SELECT payload, matched FROM source WHERE id=?', (tid,)).fetchone()
                if matched is None:
                    record_detail(writer, stats, 'extra_in_target', tid)
                elif matched[1]:
                    record_detail(writer, stats, 'pg_duplicate_ids', tid)
                else:
                    disk.execute('UPDATE source SET matched=1 WHERE id=?', (tid,))
                    if obj is not None:
                        baseline = json.loads(matched[0])
                        bad_fields = [col for col in TARGET_FIELDS if baseline.get(col) != obj.get(col)]
                        if bad_fields:
                            stats['value_mismatches'] += 1
                            for col in bad_fields:
                                record_detail(writer, stats, 'field_differences', tid, col, baseline.get(col), obj.get(col))
                if target_rows % BATCH_SIZE == 0:
                    disk.commit()
                    logging.info('Target rows inspected %d', target_rows)
            cur.close()
            disk.commit()
            for (tid,) in disk.execute('SELECT id FROM source WHERE matched=0'):
                record_detail(writer, stats, 'missing_in_target', tid)
            if source_rows != target_rows:
                record_detail(writer, stats, 'count_mismatch', '', 'COUNT', source_rows, target_rows)
        # Any aggregate failure is a hard DQ FAIL even if row comparison passes.
        for name, values in aggregates.items():
            if values['status'] != 'PASS':
                with path.open('a', newline='', encoding='utf-8') as out:
                    csv.writer(out).writerow(['aggregate_mismatch', '', name, values['mysql'], values['postgresql']])
                stats['discrepancies'] += 1
                stats['aggregate_mismatches'] = stats.get('aggregate_mismatches',0) + 1
        audit_metrics = build_audit_metrics(aggregates)
        column_evidence = build_column_evidence(types, aggregates, stats)
        passed = (source_rows == target_rows and stats['discrepancies'] == 0)
        # ADDED - DQ timing covers checks and evidence preparation, excluding downstream NEXUS import.
        completed_at_pht = pht_now()
        dq_duration_seconds = round(time.monotonic() - t0, 2)
        summary = {'run_id': dq_run_id,  # ADDED - same ID for JSON and audit table
                   'status': 'PASS' if passed else 'FAIL', 'stage': 'MySQL-DLAKE',
                   'mysql_dlake_trust_result': ('PASS_FOR_NEXUS_IMPORT' if passed else 'FAIL_NOT_READY_FOR_NEXUS_IMPORT'),
                   'window_from':args.from_dt, 'window_to':args.to_dt,
                   'started_at_pht':started_at_pht.strftime('%Y-%m-%d %H:%M:%S'),
                   'completed_at_pht':completed_at_pht.strftime('%Y-%m-%d %H:%M:%S'),
                   'duration_hhmmss':hhmmss(dq_duration_seconds),
                   'source_rows':source_rows,'target_rows':target_rows,
                   'missing_in_target':stats['missing_in_target'],
                   'value_mismatches':stats['value_mismatches'],
                   'stats':stats,'type_compatibility':types,'financial_sql_aggregates':aggregates,
                   'audit_metrics':audit_metrics, 'column_level_evidence':column_evidence,
                   'summary_report_path':str(summary_path),
                   'detail_csv':str(path), 'source_archive':args.file,
                   'duration_seconds':dq_duration_seconds,
                   'nexus_import_status':'SKIPPED' if SKIP_PROD_IMPORT or not passed else 'NOT_STARTED',
                   'nexus_import_duration_seconds':None,
                   'nexus_import_started_at_pht':None,
                   'nexus_import_completed_at_pht':None}
        summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
        save_summary_pg(audit_pg, summary)
        summary['audit_status'] = 'PASS'
        summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
        logging.info('DQ %s source=%d target=%d issues=%d reports=%s',summary['status'],source_rows,target_rows,stats['discrepancies'],summary_path)
        logging.info('DQ timing started_at_pht=%s completed_at_pht=%s duration=%s (%ss)',
                     summary['started_at_pht'], summary['completed_at_pht'],
                     summary['duration_hhmmss'], summary['duration_seconds'])
        print_summary('DQ SUMMARY', (
            (field, summary[field]) for field in
            ('status', 'mysql_dlake_trust_result', 'window_from', 'window_to', 'started_at_pht',
             'completed_at_pht', 'duration_seconds', 'duration_hhmmss',
             'source_rows', 'target_rows')
        ))
        if not passed:
            print_summary('RAW DQ FINAL HANDOFF SUMMARY', [
                ('status', 'FAIL'), ('raw_dq', 'FAIL'),
                ('next_process', 'NOT STARTED'),
                ('issues', stats['discrepancies']),
                ('window_from', args.from_dt), ('window_to', args.to_dt),
                ('report', str(summary_path)),
            ])
            notify_failure('MySQL-DLAKE DQ FAIL\nWindow: %s -> %s\nSource=%d Target=%d\nIssues=%d\nReport=%s\nDetail=%s' %
                           (args.from_dt,args.to_dt,source_rows,target_rows,stats['discrepancies'],summary_path,path))
            return 1
        if SKIP_PROD_IMPORT:
            print_summary('RAW DQ FINAL HANDOFF SUMMARY', [
                ('status', 'PASS'), ('raw_dq', 'PASS'),
                ('next_process', 'SKIPPED by SKIP_PROD_IMPORT'),
                ('window_from', args.from_dt), ('window_to', args.to_dt),
            ])
            logging.info('DQ PASS; SKIP_PROD_IMPORT=True; no downstream migration')
            return 0
        if not IMPORT_BASH.is_file():
            raise RuntimeError('DQ PASS but Data Lake -> NEXUS Bash is missing: %s' % IMPORT_BASH)
        logging.info('DQ PASS: Starting Data Lake -> NEXUS via %s', IMPORT_BASH)
        # Gold Phase starts independently; Raw DQ must not wait for publishing.
        # Release the connections and disk-backed DQ index after the audit commits.
        mysql.close()
        mysql = None
        pg.close()
        pg = None
        audit_pg.close()
        audit_pg = None
        disk.close()
        disk = None
        if sqlite_path is not None:
            sqlite_path.unlink(missing_ok=True)
            sqlite_path = None

        handoff_dir = REPORT_DIR / 'nexus_handoff'
        handoff_dir.mkdir(parents=True, exist_ok=True)
        handoff_log = handoff_dir / ('nexus_%s_%s_%s.log' % (
            start.strftime('%Y%m%d'), pht_now().strftime('%Y%m%d_%H%M%S_%f'), os.getpid()))
        command = ['/usr/bin/bash', str(IMPORT_BASH),
                   start.strftime('%Y-%m-%d'), stop.strftime('%Y-%m-%d'), 'gameTransaction',
                   '--brands', os.environ.get('PRISM_D2S_BRAND', 'inplayv1')]
        summary['nexus_import_status'] = 'STARTING'
        summary['nexus_import_started_at_pht'] = pht_now().strftime('%Y-%m-%d %H:%M:%S')
        summary['nexus_handoff_log'] = str(handoff_log)
        summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
        with handoff_log.open('ab') as out:
            child = subprocess.Popen(
                command, cwd=str(IMPORT_BASH.parent), env=os.environ.copy(),
                stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
                close_fds=True, start_new_session=True, shell=False)
        summary['nexus_handoff_pid'] = child.pid
        logging.info('NEXUS HANDOFF | STARTED | PID=%s | log=%s', child.pid, handoff_log)

        # Startup-only confirmation; Gold Phase is responsible for later failures.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            rc = child.poll()
            if rc is not None:
                if rc != 0:
                    summary['nexus_import_status'] = 'FAIL'
                    summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
                    raise RuntimeError('Gold Phase startup failed rc=%s; log=%s' % (rc, handoff_log))
                break
            time.sleep(0.25)
        rc = child.poll()
        if rc is not None and rc != 0:
            summary['nexus_import_status'] = 'FAIL'
            summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
            raise RuntimeError('Gold Phase startup failed rc=%s; log=%s' % (rc, handoff_log))
        summary['nexus_import_status'] = 'PASS' if rc == 0 else 'HANDOFF_ACCEPTED'
        summary['nexus_import_duration_seconds'] = round(time.monotonic() - t0, 2)
        summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
        print_summary('RAW DQ FINAL HANDOFF SUMMARY', [
            ('status', 'PASS'),
            ('raw_dq', 'PASS'),
            ('next_process', 'NEXUS Gold Phase'),
            ('handoff', 'SUCCESS - detached launch' if rc is None else 'SUCCESS - process exited 0'),
            ('gold_phase_status', 'RUNNING independently' if rc is None else 'COMPLETED (exit 0)'),
            ('gold_phase_pid', child.pid),
            ('business_window', '%s -> %s' % (args.from_dt, args.to_dt)),
            ('gold_phase_log', str(handoff_log)),
            ('note', 'Gold Phase and post-release verification are monitored independently'),
        ])
        logging.info('RAW DQ FINAL PASS | NEXUS handoff successful | child_pid=%s | log=%s', child.pid, handoff_log)
        return 0
    except Exception as exc:
        logging.exception('DQ / downstream orchestration failure')
        # ADDED - Preserve real execution timing even if SQL, datatype, or import fails.
        failure_end = pht_now()
        failure_duration = round(time.monotonic() - t0, 2)
        if 'summary' not in locals():
            summary = {'run_id': dq_run_id,  # ADDED - preserve ID on failure
                        'status': 'FAIL', 'stage': 'MySQL-DLAKE',
                       'mysql_dlake_trust_result': 'FAIL_NOT_READY_FOR_NEXUS_IMPORT',
                       'window_from': args.from_dt, 'window_to': args.to_dt,
                       'started_at_pht': started_at_pht.strftime('%Y-%m-%d %H:%M:%S'),
                       'completed_at_pht': failure_end.strftime('%Y-%m-%d %H:%M:%S'),
                       'duration_seconds': failure_duration,
                       'duration_hhmmss': hhmmss(failure_duration),
                       'source_rows': source_rows, 'target_rows': target_rows,
                       'error': str(exc), 'nexus_import_status': 'NOT_STARTED'}
        else:
            # The DQ validation may have passed while a separate NEXUS import failed.
            summary['pipeline_status'] = 'FAIL'
            summary['pipeline_error'] = str(exc)
            # ADDED - A DQ audit failure is a DQ failure even when data matched.
            # A downstream NEXUS failure leaves the validated DQ status as PASS,
            # but always sets pipeline_status=FAIL.
            if summary.get('nexus_import_status') in ('NOT_STARTED', 'SKIPPED'):
                summary['status'] = 'FAIL'
                summary['mysql_dlake_trust_result'] = 'FAIL_NOT_READY_FOR_NEXUS_IMPORT'
                summary['audit_status'] = 'FAIL'
                summary['error'] = str(exc)
        summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
        logging.error('Failure evidence: %s', summary_path)
        print('execution_started_at_pht : %s' % summary['started_at_pht'])
        print('execution_ended_at_pht   : %s' % failure_end.strftime('%Y-%m-%d %H:%M:%S'))
        print('execution_duration       : %s' % hhmmss(failure_duration))
        print_summary('RAW DQ FINAL HANDOFF SUMMARY', [
            ('status', 'FAIL'), ('failed_stage', 'DQ or Gold Phase startup'),
            ('error', str(exc)), ('log', str(log_path)),
            ('summary', str(summary_path)),
        ])
        notify_failure('MySQL-DLAKE DQ FAIL or downstream NEXUS import FAIL\nWindow: %s -> %s\nError: %s\nLog: %s\nSummary: %s' %
                       (args.from_dt,args.to_dt,exc,log_path,summary_path))
        return 1
    finally:
        if mysql:
            mysql.close()
        if pg:
            pg.close()
        if audit_pg:
            audit_pg.close()
        try:
            disk.close()
        except (NameError, Exception):
            pass
        if sqlite_path:
            sqlite_path.unlink(missing_ok=True)


if __name__ == '__main__':
    sys.exit(main())
