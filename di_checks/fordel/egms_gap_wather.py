#!/usr/bin/env python3
"""NEXUS/MySQL-DLAKE 7-completed-business-day count drift checker (Python 3.9).

# ADDED - READ-ONLY on source/target data.
# ADDED - Writes only monitoring observations to db_admin.etl_source_window_checks.
# ADDED - Sends email only for new/changed/reappeared alerts; never backfills.
"""
import argparse
import fcntl
import hashlib
import logging
import os
import smtplib
import sys
import time
from datetime import datetime, date, timedelta, time as dtime, timezone
from email.message import EmailMessage
from pathlib import Path
from zoneinfo import ZoneInfo

import pymysql
import psycopg2
from psycopg2 import sql

# =============================================================================
# UPDATED - CHANGE VARIABLES HERE
# Explicit settings match egms_games_txn_mysql_extract_utc_source.py.
# You can edit host/user/database here, or override via argparse CLI flags.
# Passwords are deliberately NOT hard-coded: use environment variables.
# =============================================================================

# MySQL/MariaDB source (same as the working extractor).
MYSQL_HOST = "103.253.145.33"           # remote MySQL/MariaDB IP or DNS
MYSQL_PORT = 3306
MYSQL_USER = "AllanFaylona"              # use a read-only account
MYSQL_PASSWORD = os.getenv("DQ_MYSQL_PASSWORD", "")

DEFAULT_DB = "Artemis"
DEFAULT_TABLE = "EGMS_Game_Trans"
DEFAULT_DATE_COLUMN = "GameDate"         # strict 06:00 to next-day 06:00
MYSQL_DB = DEFAULT_DB                     # retained alias for existing functions
MYSQL_TABLE = DEFAULT_TABLE

# PostgreSQL Data Lake; no changes to target records.
PG_HOST = "iest-db-postgresql.cvmg4ca8uhd2.ap-southeast-1.rds.amazonaws.com"
PG_PORT = 5432
PG_USER = "YOUR_READONLY_DQ_USER"        # monitor role; needs db_admin INSERT/UPDATE
PG_PASSWORD = os.getenv("DQ_PG_PASSWORD", "")
PG_DB = "iestdl"
PG_SCHEMA = "public"
PG_TABLE = "GameTransactionInplayV1_dmp"

# The monitor compares MySQL GameDate with PG game_dt.
# The existing extractor labels source PHT clock values as UTC.
# Maintain the SAME 06:00 clock boundaries: do NOT apply an 8-hour shift.
OUTPUT_ID_COLUMN = "TransactionID"      # informational mapping only
GAME_DT_OUTPUT_COLUMN = "GameDate"       # informational mapping only

# DQ tables and stage; checker writes observations only to DQ_CHECK_TABLE.
DQ_SCHEMA = "db_admin"
DQ_SUMMARY_TABLE = "etl_dq_run_summary"  # reads previous validated counts
DQ_CHECK_TABLE = "etl_source_window_checks" # writes rolling checks
ETL_STAGE = "MySQL-DLAKE"
BRAND = "inplayv1"
ENTITY = "game_transaction"

# Files and runtime controls (no extracted transaction files created).
FILE_PREFIX = "egms_late_check"
BASE_DIR = Path("/home/allanf/scripts/artem")
LOG_DIR = BASE_DIR / "logs"
LOCK_FILE = BASE_DIR / "egms_late_arrival_checker.lock"
LOOKBACK_DAYS = 7
SLEEP_SECONDS = 0.15
ALERT_ON_MISSING_BASELINE = True

# SMTP (email only on new/changed alerts or errors).
SMTP_HOST = "YOUR_SMTP_HOST"             # set '' to disable until configured
SMTP_PORT = 587
SMTP_USER = "YOUR_SMTP_USERNAME"
SMTP_PASSWORD = os.getenv("DQ_SMTP_PASSWORD", "")
SMTP_FROM = "etl-alerts@example.com"
SMTP_TO = "your-address@example.com"    # multiple: comma-separated
SMTP_STARTTLS = True
SMTP_TIMEOUT_SECONDS = 20


def args_parse():
    p = argparse.ArgumentParser(description='Read-only rolling late-data detection; no backfill.')
    p.add_argument('--lookback-days', type=int, default=LOOKBACK_DAYS)
    p.add_argument('--as-of-date', help='PHT date YYYY-MM-DD for reproducible checks (default: today PHT)')
    p.add_argument('--mysql-host', default=MYSQL_HOST)
    p.add_argument('--mysql-port', type=int, default=MYSQL_PORT)
    p.add_argument('--mysql-user', default=MYSQL_USER)
    p.add_argument('--mysql-db', default=MYSQL_DB)
    p.add_argument('--mysql-table', default=MYSQL_TABLE)
    p.add_argument('--pg-host', default=PG_HOST)
    p.add_argument('--pg-port', type=int, default=PG_PORT)
    p.add_argument('--pg-user', default=PG_USER)
    p.add_argument('--pg-database', default=PG_DB)
    p.add_argument('--pg-schema', default=PG_SCHEMA)
    p.add_argument('--pg-table', default=PG_TABLE)
    p.add_argument('--sleep-seconds', type=float, default=SLEEP_SECONDS)
    p.add_argument('--alert-on-missing-baseline', choices=['yes','no'],
                   default='yes' if ALERT_ON_MISSING_BASELINE else 'no')
    p.add_argument('--no-email', action='store_true', help='Disable SMTP; monitor and log only')
    p.add_argument('--check-only', action='store_true', help='Connections/table metadata only; no counts or writes')
    a = p.parse_args()
    if not 1 <= a.lookback_days <= 60 or a.sleep_seconds < 0:
        p.error('lookback-days must be 1..60 and sleep-seconds >= 0')
    # Validate identifiers without dynamically concatenating SQL injection strings.
    for val in (a.mysql_db, a.mysql_table):
        if not val or not val.replace('_','').isalnum():
            p.error('MySQL database/table must be alphanumeric/underscore identifiers')
    return a


def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / ('egms_late_check_%s.log' % datetime.now(ZoneInfo('Asia/Manila')).strftime('%Y%m%d_%H%M%S'))
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s',
                        handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)])
    return log_path


def connect_mysql(a):
    return pymysql.connect(host=a.mysql_host, port=a.mysql_port, user=a.mysql_user,
                           password=MYSQL_PASSWORD, database=a.mysql_db, charset='utf8mb4',
                           autocommit=True, connect_timeout=12, read_timeout=300,
                           cursorclass=pymysql.cursors.DictCursor)


def connect_pg(a):
    return psycopg2.connect(host=a.pg_host, port=a.pg_port, user=a.pg_user,
                            password=PG_PASSWORD, dbname=a.pg_database,
                            sslmode='require', connect_timeout=12,
                            options='-c statement_timeout=300000 -c lock_timeout=5000')


def completed_windows(a):
    now_pht = datetime.now(ZoneInfo('Asia/Manila'))
    if a.as_of_date:
        today = date.fromisoformat(a.as_of_date)
        # Explicit as-of dates represent a 06:00 PHT boundary for test/replay.
        boundary_date = today
    else:
        boundary_date = now_pht.date() if now_pht.time() >= dtime(6, 0) else now_pht.date() - timedelta(days=1)
    for n in range(a.lookback_days, 0, -1):
        d = boundary_date - timedelta(days=n)
        # IMPORTANT: compare the SAME *clock values* in MySQL and PG UTC-labeled field.
        # This follows the pre-existing extractor convention; not a timezone conversion.
        yield d, datetime.combine(d, dtime(6, 0)), datetime.combine(d + timedelta(days=1), dtime(6, 0))


def read_source_count(mysql, a, start, end):
    q = ('SELECT COUNT(*) AS cnt FROM `%s`.`%s` '
         'WHERE `GameDate` >= %%s AND `GameDate` < %%s') % (a.mysql_db, a.mysql_table)
    with mysql.cursor() as c:
        c.execute(q, (start, end))
        return int(c.fetchone()['cnt'])


def read_pg_count(pg, a, start, end):
    q = sql.SQL('SELECT COUNT(*) FROM {}.{} WHERE game_dt >= (%s::timestamp AT TIME ZONE \'UTC\') '
                'AND game_dt < (%s::timestamp AT TIME ZONE \'UTC\')').format(
                    sql.Identifier(a.pg_schema), sql.Identifier(a.pg_table))
    with pg.cursor() as c:
        c.execute(q, (start, end))
        return int(c.fetchone()[0])


def read_baseline(pg, start, end):
    q = sql.SQL('SELECT id, source_rows, target_rows, status FROM {}.{} '
                'WHERE etl_stage=%s AND brand=%s AND entity=%s AND window_from=%s AND window_to=%s '
                'AND status IN (\'PASS\',\'PASS_WITH_ROUNDING_NOTE\') '
                'ORDER BY created_at DESC, id DESC LIMIT 1').format(
                    sql.Identifier(DQ_SCHEMA),sql.Identifier(DQ_SUMMARY_TABLE))
    with pg.cursor() as c:
        c.execute(q, (ETL_STAGE, BRAND, ENTITY, start, end))
        return c.fetchone()


def latest_observation(pg, start, end):
    q = sql.SQL('SELECT status, alert_fingerprint, notification_status FROM {}.{} '
                'WHERE etl_stage=%s AND brand=%s AND entity=%s AND window_from=%s AND window_to=%s '
                'ORDER BY check_id DESC LIMIT 1').format(
                    sql.Identifier(DQ_SCHEMA),sql.Identifier(DQ_CHECK_TABLE))
    with pg.cursor() as c:
        c.execute(q, (ETL_STAGE, BRAND, ENTITY, start, end))
        return c.fetchone()


def record_observation(pg, start, end, baseline, source, target, status, notes, fingerprint):
    q = sql.SQL('INSERT INTO {}.{} '
                '(etl_stage,brand,entity,business_date,window_from,window_to,baseline_dq_run_id,'
                'previous_source_rows,current_source_rows,current_target_rows,source_count_delta,'
                'source_target_delta,status,notes,alert_fingerprint) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING check_id').format(
                    sql.Identifier(DQ_SCHEMA),sql.Identifier(DQ_CHECK_TABLE))
    prev = baseline[1] if baseline else None
    vals = (ETL_STAGE, BRAND, ENTITY, start.date(), start, end,
            baseline[0] if baseline else None, prev, source, target,
            source-prev if source is not None and prev is not None else None,
            source-target if source is not None and target is not None else None,
            status, notes, fingerprint)
    with pg.cursor() as c:
        c.execute(q, vals)
        check_id = c.fetchone()[0]
    pg.commit()
    return check_id


def email_alert(a, alerts):
    if a.no_email:
        return 'DISABLED'
    if (not SMTP_HOST or SMTP_HOST.startswith('YOUR_') or
            not SMTP_FROM or SMTP_FROM.endswith('@example.com') or
            not SMTP_TO or SMTP_TO.endswith('@example.com')):
        logging.error('EMAIL NOT SENT: configure DQ_SMTP_HOST, DQ_EMAIL_FROM, DQ_EMAIL_TO')
        return 'NOT_CONFIGURED'
    msg = EmailMessage()
    msg['Subject'] = '[NEXUS][MySQL-DLAKE] Late-data review required (%d window(s))' % len(alerts)
    msg['From'] = SMTP_FROM
    msg['To'] = SMTP_TO
    lines = ['Rolling seven-day source check detected changes/errors.',
             'This checker DOES NOT perform backfill.', '']
    for item in alerts:
        lines.append('Window %s to %s | status=%s | old=%s | MySQL=%s | DataLake=%s | check_id=%s' % item)
    lines.extend(['', 'Action: investigate affected business-day transactions and run controlled backfill/DQ.',
                  'Normal scheduled extraction remains independent.'])
    msg.set_content('\n'.join(lines))
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS) as s:
            if SMTP_STARTTLS:
                s.starttls()
            if SMTP_USER:
                s.login(SMTP_USER, SMTP_PASSWORD)
            s.send_message(msg)
        return 'SENT'
    except Exception:
        logging.exception('SMTP alert failed')
        return 'FAILED'


def mark_email(pg, ids, status):
    if not ids:
        return
    q = sql.SQL('UPDATE {}.{} SET notification_status=%s, notified_at=CASE WHEN %s=\'SENT\' THEN now() ELSE NULL END '
                'WHERE check_id = ANY(%s)').format(sql.Identifier(DQ_SCHEMA),sql.Identifier(DQ_CHECK_TABLE))
    with pg.cursor() as c:
        c.execute(q, (status,status,ids))
    pg.commit()


def main():
    a = args_parse()
    setup_logging()
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    start_t = time.monotonic()
    with LOCK_FILE.open('a+') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logging.error('Another checker instance is already running')
            return 2
        logging.info('START | %d complete business days | MySQL=%s.%s | PG=%s.%s',
                     a.lookback_days, a.mysql_db, a.mysql_table, a.pg_schema, a.pg_table)
        mysql = pg = None
        alerts, alert_ids = [], []
        findings = 0
        try:
            mysql = connect_mysql(a)
            pg = connect_pg(a)
            if a.check_only:
                with mysql.cursor() as c:
                    c.execute('SELECT 1')
                with pg.cursor() as c:
                    c.execute('SELECT 1')
                    c.execute('SELECT to_regclass(%s),to_regclass(%s),to_regclass(%s)',
                              ('%s.%s'%(DQ_SCHEMA,DQ_SUMMARY_TABLE),
                               '%s.%s'%(DQ_SCHEMA,DQ_CHECK_TABLE),
                               '%s.%s'%(a.pg_schema,a.pg_table)))
                    if any(x is None for x in c.fetchone()):
                        raise RuntimeError('One or more PostgreSQL tables are missing')
                print('CHECK ONLY PASS')
                return 0
            for day, start, end in completed_windows(a):
                baseline = None
                source = target = None
                try:
                    # A count is fast relative to full-column DQ, but it still reads an index range.
                    baseline = read_baseline(pg, start, end)
                    source = read_source_count(mysql, a, start, end)
                    target = read_pg_count(pg, a, start, end)
                    if baseline is None:
                        status = 'BASELINE_MISSING' if a.alert_on_missing_baseline=='yes' else 'NO_BASELINE'
                    elif source != target or source != baseline[1] or target != baseline[2]:
                        status = 'CATCHUP_REQUIRED'
                    else:
                        status = 'NO_COUNT_DRIFT'
                    notes = 'Count-only screening; matching counts do not prove matching IDs/values.'
                except Exception as e:
                    pg.rollback()
                    logging.exception('Window %s check failed', day)
                    status = 'CHECK_ERROR'
                    notes = str(e)[:1500]
                fp_text = '%s|%s|%s|%s' % (status,baseline[1] if baseline else None,source,target)
                fp = hashlib.sha256(fp_text.encode()).hexdigest() if status in ('CATCHUP_REQUIRED','BASELINE_MISSING','CHECK_ERROR') else None
                previous = latest_observation(pg, start, end)
                check_id = record_observation(pg, start, end, baseline, source, target, status, notes, fp)
                logging.info('WINDOW %s | baseline=%s source=%s target=%s status=%s check_id=%s',
                             day,baseline[1] if baseline else None,source,target,status,check_id)
                if fp:
                    findings += 1
                    # New/changed/reappeared only; previous NO_COUNT_DRIFT resets alert.
                    if previous is None or previous[1] != fp or previous[2] != 'SENT':
                        alerts.append((start,end,status,baseline[1] if baseline else None,source,target,check_id))
                        alert_ids.append(check_id)
                if a.sleep_seconds:
                    time.sleep(a.sleep_seconds)
            email_status = email_alert(a, alerts) if alerts else 'NONE_NEW'
            mark_email(pg, alert_ids, email_status)
            logging.info('END | checked=%s findings=%s new_alerts=%s email=%s duration=%.1fs',
                         a.lookback_days, findings, len(alerts), email_status, time.monotonic()-start_t)
            print('ROLLING CHECK: %d review windows; %d new alert(s); email=%s' % (findings,len(alerts),email_status))
            # Catch-up required is advisory, not extractor gate; errors are nonzero.
            return 2 if findings and any(x[2]=='CHECK_ERROR' for x in alerts) else 0
        except Exception:
            logging.exception('FATAL: checker failed before completing window checks')
            # A fatal connection/schema failure cannot be recorded in monitoring table.
            email_alert(a,[('GLOBAL','GLOBAL','CHECK_ERROR',None,None,None,'unavailable')])
            return 2
        finally:
            if mysql: mysql.close()
            if pg: pg.close()


if __name__ == '__main__':
    sys.exit(main())

